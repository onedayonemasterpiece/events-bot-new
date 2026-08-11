#!/usr/bin/env python3
"""Reject carrier-empty instructions on live source-parse prompt surfaces.

The Telegram Monitor file still contains the large legacy ``extract_events``
implementation for rollback archaeology.  It is deliberately *not* a live
surface: both producer call sites use ``extract_source_parse_decision``.  This
audit therefore extracts only ``_source_parse_prompt`` and also proves that no
call to ``extract_events`` exists outside the legacy function itself.  If that
call graph changes, CI fails before legacy prompt text can become reachable.
"""

from __future__ import annotations

import argparse
import ast
from pathlib import Path
import re
import sys
import unicodedata


DISPOSITIONS = {
    "EVENTS_FOUND",
    "CONFIRMED_NO_EVENT",
    "LIFECYCLE_ONLY",
    "MIXED",
    "RETRY_REQUIRED",
}


def _normalise(value: str) -> str:
    text = unicodedata.normalize("NFKC", value).casefold().replace("ё", "е")
    text = text.replace("`", "").replace("‘", "'").replace("’", "'")
    return re.sub(r"\s+", " ", text)


LEGACY_EMPTY_PATTERNS = (
    re.compile(r"\breturn\s*\[\s*\]"),
    re.compile(r"\bверни(?:те)?\s*\[\s*\]"),
    re.compile(r"\bfail\s*closed\s*\(?\s*\[\s*\]\s*\)?"),
    re.compile(r"\b(?:return|respond\s+with)\s+no\s+(?:future\s+)?events?\b"),
    re.compile(r"\bпуст(?:ой|ого|ым|ые|ую)\s+(?:json\s+)?массив"),
    re.compile(r"\b(?:верни(?:те)?|возврати(?:те)?)\s+никаких\s+событий\b"),
)


def _string_constants(node: ast.AST) -> list[str]:
    return [
        item.value
        for item in ast.walk(node)
        if isinstance(item, ast.Constant) and isinstance(item.value, str)
    ]


def _named_function(tree: ast.Module, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise ValueError(f"missing function {name}")


def _master_prompt(path: Path) -> str:
    source = path.read_text(encoding="utf-8")
    match = re.search(
        r"```[^\n]*\n(?P<prompt>MASTER-PROMPT\b.*?)\n```",
        source,
        flags=re.DOTALL,
    )
    if not match:
        raise ValueError("missing fenced MASTER-PROMPT")
    return match.group("prompt")


def _vk_live_prompt(path: Path) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function = _named_function(tree, "build_event_drafts_from_vk")
    fragments: list[str] = []
    for node in ast.walk(function):
        value: ast.AST | None = None
        if isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "llm_text":
                value = node.value
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id == "llm_text" for target in targets):
                value = node.value
        if value is not None:
            fragments.extend(_string_constants(value))
    if not fragments:
        raise ValueError("no live llm_text fragments found in build_event_drafts_from_vk")
    return "\n".join(fragments)


def _tg_live_prompt(path: Path) -> tuple[str, bool]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function = _named_function(tree, "_source_parse_prompt")
    fragments = _string_constants(function)

    # The live function deliberately serialises these closed sets rather than
    # duplicating their values in prose. Include their literal definitions in
    # the audited surface.
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        names = {target.id for target in node.targets if isinstance(target, ast.Name)}
        if names & {
            "SOURCE_PARSE_DISPOSITIONS",
            "SOURCE_PARSE_RETRY_REASONS",
            "SOURCE_PARSE_NO_EVENT_REASONS",
            "SOURCE_PARSE_LIFECYCLE_ACTIONS",
        }:
            fragments.extend(_string_constants(node.value))

    legacy_reachable = False
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "extract_events":
            continue
        if any(
            isinstance(item, ast.Call)
            and isinstance(item.func, ast.Name)
            and item.func.id == "extract_events"
            for item in ast.walk(node)
        ):
            legacy_reachable = True
            break
    return "\n".join(fragments), legacy_reachable


def extract_live_prompts(root: Path) -> dict[str, str]:
    tg_prompt, legacy_reachable = _tg_live_prompt(
        root / "kaggle/TelegramMonitor/telegram_monitor.py"
    )
    prompts = {
        "docs:MASTER-PROMPT": _master_prompt(root / "docs/llm/prompts.md"),
        "vk:build_event_drafts_from_vk": _vk_live_prompt(root / "vk_intake.py"),
        "tg:_source_parse_prompt": tg_prompt,
    }
    if legacy_reachable:
        prompts["tg:legacy-call-graph-error"] = "extract_events is reachable"
    return prompts


def find_violations(root: Path) -> list[str]:
    try:
        prompts = extract_live_prompts(root)
    except (OSError, SyntaxError, ValueError) as exc:
        return [f"prompt extraction failed: {exc}"]

    violations: list[str] = []
    if "tg:legacy-call-graph-error" in prompts:
        violations.append(
            "Telegram legacy extract_events became reachable; it must enter the live prompt audit"
        )

    for surface, prompt in prompts.items():
        if surface.endswith("call-graph-error"):
            continue
        normal = _normalise(prompt)
        for pattern in LEGACY_EMPTY_PATTERNS:
            if pattern.search(normal):
                violations.append(
                    f"{surface}: legacy carrier-empty instruction matches {pattern.pattern!r}"
                )
        missing = sorted(token for token in DISPOSITIONS if token.casefold() not in normal)
        if missing:
            violations.append(f"{surface}: missing typed dispositions: {', '.join(missing)}")
        if "evidence_incomplete" not in normal:
            violations.append(f"{surface}: missing EVIDENCE_INCOMPLETE retry policy")

    # The shared master and VK overlay carry the complete A-G semantic case
    # contract. Telegram's JSON prompt carries the same closed verdict model
    # plus generic complete-evidence/giveaway/lifecycle rules.
    for surface in ("docs:MASTER-PROMPT", "vk:build_event_drafts_from_vk"):
        normal = _normalise(prompts[surface])
        policy_groups = {
            "giveaway-only versus giveaway+event": ("giveaway", "розыгрыш"),
            "closed giveaway-only reason": ("no_event_reason=giveaway_only",),
            "incomplete-card retry": ("incomplete", "неполн"),
            "positive enrichment": ("enrichment",),
            "vague teaser": ("teaser", "тизер"),
            "lifecycle-only": ("lifecycle_only",),
        }
        for label, alternatives in policy_groups.items():
            if not any(value in normal for value in alternatives):
                violations.append(f"{surface}: missing typed policy {label}")

    tg_normal = _normalise(prompts["tg:_source_parse_prompt"])
    for label, alternatives in {
        "complete raw/OCR evidence": ("complete raw text", "complete source text"),
        "every OCR block": ("every ocr block",),
        "giveaway mixed-content preservation": ("giveaway",),
        "lifecycle actions": ("lifecycle_actions",),
        "technical uncertainty retry": ("technical uncertainty",),
    }.items():
        if not any(value in tg_normal for value in alternatives):
            violations.append(f"tg:_source_parse_prompt: missing policy {label}")
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    violations = find_violations(args.root.resolve())
    if violations:
        print("source-parse prompt contract FAILED", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1
    print("source-parse prompt contract OK (3 live surfaces; legacy TG extractor unreachable)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
