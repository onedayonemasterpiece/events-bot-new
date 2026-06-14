from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

from .config import RunConfig
from .contracts import Candidate, SemanticPlan
from .dependencies import dependency_report
from .llm_gateway import gateway_status


def write_candidate_meta(candidate: Candidate) -> Path:
    if not candidate.svg_path:
        raise ValueError("candidate.svg_path is required before metadata export")
    meta_path = candidate.svg_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(candidate.to_meta(), ensure_ascii=False, indent=2), encoding="utf-8")
    candidate.meta_path = meta_path
    return meta_path


def write_leaderboard(candidates: list[Candidate], out_path: str | Path) -> Path:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "rank",
                "candidate_id",
                "variant",
                "family",
                "accepted",
                "final_eligible",
                "primitive_rendered",
                "proposal_only",
                "cv_score",
                "gemini_score",
                "path_count",
                "failure_flags",
                "warnings",
            ],
        )
        writer.writeheader()
        for idx, candidate in enumerate(candidates, start=1):
            writer.writerow(
                {
                    "rank": idx,
                    "candidate_id": candidate.candidate_id,
                    "variant": candidate.variant,
                    "family": candidate.family,
                    "accepted": candidate.accepted,
                    "final_eligible": candidate.final_eligible,
                    "primitive_rendered": candidate.primitive_rendered,
                    "proposal_only": candidate.proposal_only,
                    "cv_score": candidate.cv_score,
                    "gemini_score": candidate.gemini_score if candidate.gemini_score is not None else "",
                    "path_count": len(candidate.lines),
                    "failure_flags": ";".join(candidate.failure_flags),
                    "warnings": ";".join(candidate.warnings),
                }
            )
    return out


def export_final(best: Candidate, ranked: list[Candidate], config: RunConfig, semantic_plan: SemanticPlan, warnings: list[str]) -> dict[str, str | None]:
    if not best.final_eligible or not best.primitive_rendered:
        raise RuntimeError("final.svg can only be exported from a primitive-rendered final-eligible candidate")
    out_dir = config.resolved_output_dir()
    top_dir = out_dir / "top_alternatives"
    top_dir.mkdir(parents=True, exist_ok=True)
    exported: dict[str, str | None] = {}
    if best.svg_path:
        final_svg = out_dir / "final.svg"
        shutil.copyfile(best.svg_path, final_svg)
        exported["final_svg"] = str(final_svg)
    if best.preview_path and best.preview_path.exists():
        final_png = out_dir / "preview.png"
        shutil.copyfile(best.preview_path, final_png)
        exported["preview_png"] = str(final_png)
    for rank, candidate in enumerate(ranked[: config.candidate_search.final_top_k], start=1):
        if candidate.svg_path:
            shutil.copyfile(candidate.svg_path, top_dir / f"rank_{rank:02d}_{candidate.candidate_id}.svg")
        if candidate.preview_path and candidate.preview_path.exists():
            shutil.copyfile(candidate.preview_path, top_dir / f"rank_{rank:02d}_{candidate.candidate_id}.png")
        if candidate.meta_path and candidate.meta_path.exists():
            shutil.copyfile(candidate.meta_path, top_dir / f"rank_{rank:02d}_{candidate.candidate_id}.meta.json")
    final_meta = {
        "winner": best.to_meta(),
        "ranked_candidate_ids": [c.candidate_id for c in ranked],
        "semantic_plan": semantic_plan.to_dict(),
        "warnings": warnings,
        "config": config.to_dict(),
        "dependency_report": dependency_report(),
        "llm_gateway": gateway_status(config.gemini.api_key_env),
    }
    meta_path = out_dir / "final.meta.json"
    meta_path.write_text(json.dumps(final_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    exported["final_meta"] = str(meta_path)
    ranking_report = out_dir / "ranking_report.json"
    ranking_report.write_text(
        json.dumps(
            {
                "winner": best.candidate_id,
                "candidates": [c.to_meta() for c in ranked],
                "warnings": warnings,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    exported["ranking_report"] = str(ranking_report)
    return exported
