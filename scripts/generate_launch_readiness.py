#!/usr/bin/env python3
"""Generate the 2026-09-01 launch-readiness Markdown views.

The editable source is docs/release/2026-09-01/checklist.toml.
Only Python's standard library is required.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_DIR = REPO_ROOT / "docs" / "release" / "2026-09-01"
SOURCE = RELEASE_DIR / "checklist.toml"

STATUS = {
    "DONE": ("✅", "готово"),
    "IN_PROGRESS": ("🛠", "в работе"),
    "BLOCKED": ("⛔", "заблокировано"),
    "NOT_STARTED": ("○", "не начато"),
    "OWNER_GATE": ("🧭", "решение владельца"),
    "VERIFY": ("🧪", "требует проверки"),
    "DEFERRED": ("⏸", "отложено"),
}
PRIORITY = {"P0", "P1", "P2"}
STAGES = {"research", "decision", "design", "development", "integration", "qa", "live", "ready"}
EVIDENCE = {"E0", "E1", "E2", "E3", "E4", "E5"}
PHASES = {
    "PRELAUNCH",
    "FG_PREP",
    "FG_ACTIVE",
    "FG_CLOSE",
    "LAUNCH_PREP",
    "RC",
    "STABILIZATION",
    "D0",
    "POST_LAUNCH",
}
EVIDENCE_TEXT = {
    "E0": "нет проверяемого evidence",
    "E1": "документ / исследование / принятое решение",
    "E2": "код / unit / contract",
    "E3": "интеграция / browser",
    "E4": "hosted target / immutable candidate / device",
    "E5": "production / live / soak",
}
STAGE_TEXT = {
    "research": "исследование",
    "decision": "решение",
    "design": "дизайн",
    "development": "разработка",
    "integration": "интеграция",
    "qa": "тестирование",
    "live": "production/live",
    "ready": "готово",
}


def load() -> dict[str, Any]:
    with SOURCE.open("rb") as handle:
        return tomllib.load(handle)


def validate(data: dict[str, Any]) -> None:
    errors: list[str] = []
    meta = data.get("meta", {})
    items = data.get("item", [])
    if not items:
        errors.append("checklist has no items")

    ids: set[str] = set()
    used_streams: set[str] = set()
    by_id: dict[str, dict[str, Any]] = {}
    required = {
        "id",
        "stream",
        "title",
        "priority",
        "phase",
        "stage",
        "status",
        "evidence",
        "owner",
        "target",
        "source",
        "next_action",
        "notes",
        "blocked_by",
    }
    for index, item in enumerate(items, 1):
        missing = sorted(required - item.keys())
        if missing:
            errors.append(f"item #{index} missing fields: {', '.join(missing)}")
            continue
        item_id = item["id"]
        if item_id in ids:
            errors.append(f"duplicate id: {item_id}")
        ids.add(item_id)
        by_id[item_id] = item
        used_streams.add(item["stream"])
        if item["status"] not in STATUS:
            errors.append(f"{item_id}: invalid status {item['status']}")
        if item["priority"] not in PRIORITY:
            errors.append(f"{item_id}: invalid priority {item['priority']}")
        if item["stage"] not in STAGES:
            errors.append(f"{item_id}: invalid stage {item['stage']}")
        if item["evidence"] not in EVIDENCE:
            errors.append(f"{item_id}: invalid evidence {item['evidence']}")
        if item["phase"] not in PHASES:
            errors.append(f"{item_id}: invalid phase {item['phase']}")
        if item["priority"] == "P0" and item["status"] == "DEFERRED":
            errors.append(f"{item_id}: P0 cannot be DEFERRED")
        if item["status"] == "DONE" and item["evidence"] == "E0":
            errors.append(f"{item_id}: DONE requires evidence")
        if not item["next_action"].strip():
            errors.append(f"{item_id}: next_action is empty")

    for item in items:
        for dependency in item.get("blocked_by", []):
            if dependency not in ids:
                errors.append(f"{item['id']}: unknown blocked_by id {dependency}")

    stream_order = meta.get("stream_order", [])
    missing_streams = sorted(used_streams - set(stream_order))
    duplicate_streams = [name for name, count in Counter(stream_order).items() if count > 1]
    if missing_streams:
        errors.append(f"stream_order missing: {', '.join(missing_streams)}")
    if duplicate_streams:
        errors.append(f"stream_order duplicates: {', '.join(duplicate_streams)}")

    for item_id in meta.get("headline_ids", []):
        if item_id not in ids:
            errors.append(f"unknown headline id: {item_id}")

    active_p0 = [item for item in items if item["priority"] == "P0" and item["status"] != "DEFERRED"]
    unfinished_p0 = [item for item in active_p0 if item["status"] != "DONE"]
    if meta.get("verdict") == "GO" and unfinished_p0:
        errors.append("verdict=GO while unfinished P0 items exist")

    if errors:
        raise ValueError("Invalid launch checklist:\n- " + "\n- ".join(errors))


def esc(text: str) -> str:
    return str(text).replace("|", r"\|").replace("\n", " ")


def status_label(code: str) -> str:
    icon, label = STATUS[code]
    return f"{icon} {label}"


def item_link(item_id: str) -> str:
    return f"[`{item_id}`](CHECKLIST.md#{item_id.lower()})"


def stream_summary(items: list[dict[str, Any]]) -> tuple[str, Counter[str], int, int]:
    active = [item for item in items if item["status"] != "DEFERRED"]
    p0 = [item for item in active if item["priority"] == "P0"]
    counts = Counter(item["status"] for item in active)
    done_p0 = sum(item["status"] == "DONE" for item in p0)
    if p0 and all(item["status"] == "DONE" for item in p0):
        state = "🟢 READY"
    elif any(item["status"] == "BLOCKED" for item in p0):
        state = "🔴 BLOCKED"
    elif any(item["status"] in {"NOT_STARTED", "OWNER_GATE"} for item in p0):
        state = "🟠 OPEN"
    elif any(item["status"] == "VERIFY" for item in p0):
        state = "🟡 VERIFY"
    else:
        state = "🔵 IN PROGRESS"
    return state, counts, done_p0, len(p0)


def render_readme(data: dict[str, Any]) -> str:
    meta = data["meta"]
    items = data["item"]
    by_id = {item["id"]: item for item in items}
    stream_order = meta["stream_order"]
    by_stream: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        by_stream[item["stream"]].append(item)

    updated = date.fromisoformat(meta["updated_at"])
    target = date.fromisoformat(meta["target_launch"])
    days = (target - updated).days

    active = [item for item in items if item["status"] != "DEFERRED"]
    p0 = [item for item in active if item["priority"] == "P0"]
    p0_done = [item for item in p0 if item["status"] == "DONE"]
    p0_blocked = [item for item in p0 if item["status"] == "BLOCKED"]
    p0_verify = [item for item in p0 if item["status"] == "VERIFY"]
    owner_gates = [item for item in active if item["status"] == "OWNER_GATE"]
    live_evidence = [
        item
        for item in p0
        if item["evidence"] in {"E4", "E5"} and item["status"] in {"DONE", "VERIFY"}
    ]

    status_counts = Counter(item["status"] for item in active)
    lines: list[str] = []
    lines.extend(
        [
            "<!-- GENERATED: edit checklist.toml, not this file. -->",
            f"# {meta['title']} — сводный readiness dashboard",
            "",
            f"> **Срез:** {meta['updated_at']} · **до 1 сентября:** {days} дней · "
            f"**следующее обновление:** {meta['next_review']}  ",
            f"> **Фокус-группа:** cutoff {meta['focus_cutoff']}  ",
            f"> **Release verdict:** **{meta['verdict']}** — {meta['verdict_reason']}",
            "",
            "[Детальный checklist](CHECKLIST.md) · [Kanban](KANBAN.md) · "
            "[Как обновлять](UPDATE.md) · [Источник данных](checklist.toml)",
            "",
            "## Общая картина",
            "",
            "| Показатель | Текущий срез |",
            "|---|---:|",
            f"| P0 закрыто | **{len(p0_done)} / {len(p0)}** |",
            f"| P0 явно заблокировано | **{len(p0_blocked)}** |",
            f"| P0 требует hosted/live проверки | **{len(p0_verify)}** |",
            f"| Решений владельца | **{len(owner_gates)}** |",
            f"| P0 с E4/E5 evidence (готово или verify) | **{len(live_evidence)}** |",
            f"| Всего детальных пунктов | **{len(items)}** |",
            "",
            "Процент здесь намеренно не «взвешивает» исследования, код и production: "
            "пункт считается закрытым только в статусе `DONE`. `VERIFY` означает, что "
            "код или прежнее evidence есть, но актуальный target ещё не доказан.",
            "",
            "## Критический путь",
            "",
        ]
    )

    for item_id in meta.get("headline_ids", []):
        item = by_id[item_id]
        checkbox = "x" if item["status"] == "DONE" else " "
        lines.append(
            f"- [{checkbox}] {item_link(item_id)} **{status_label(item['status'])} · "
            f"{item['priority']} · {STAGE_TEXT[item['stage']]} · {item['evidence']}** — "
            f"{item['title']}. **Далее:** {item['next_action']}"
        )

    lines.extend(
        [
            "",
            "## План фаз",
            "",
            "> Окна до D0 — рабочее предложение. Они становятся обязательством только после "
            "owner-решения `GOV-03`; дата публичного запуска 1 сентября зафиксирована.",
            "",
            "| Фаза | Окно | Результат | Статус |",
            "|---|---|---|---|",
        ]
    )
    for milestone in data.get("milestone", []):
        lines.append(
            f"| `{esc(milestone['id'])}` | {esc(milestone['window'])} | "
            f"{esc(milestone['title'])} | `{esc(milestone['status'])}` |"
        )

    lines.extend(
        [
            "",
            "## Укрупнённый checklist",
            "",
            "| Контур | Состояние | P0 закрыто | Blocked | Verify | Owner gate |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for stream_index, stream in enumerate(stream_order, 1):
        state, counts, done_p0, total_p0 = stream_summary(by_stream[stream])
        anchor = f"stream-{stream_index:02d}"
        lines.append(
            f"| [{esc(stream)}](CHECKLIST.md#{anchor}) | {state} | "
            f"{done_p0}/{total_p0} | {counts['BLOCKED']} | {counts['VERIFY']} | "
            f"{counts['OWNER_GATE']} |"
        )

    lines.extend(
        [
            "",
            "## Где находится работа по стадиям",
            "",
            "| Стадия | Открыто | Что означает |",
            "|---|---:|---|",
        ]
    )
    for stage in ["research", "decision", "design", "development", "integration", "qa", "live"]:
        count = sum(
            item["stage"] == stage and item["status"] not in {"DONE", "DEFERRED"}
            for item in items
        )
        lines.append(f"| {STAGE_TEXT[stage]} | {count} | Незакрытые пункты текущей стадии |")

    lines.extend(
        [
            "",
            "## Визуальный срез",
            "",
            "```mermaid",
            "pie showData",
            '    title Активные пункты по статусам',
        ]
    )
    for code in ["DONE", "IN_PROGRESS", "VERIFY", "OWNER_GATE", "BLOCKED", "NOT_STARTED"]:
        lines.append(f'    "{STATUS[code][1]}" : {status_counts[code]}')
    lines.extend(
        [
            "```",
            "",
            "```mermaid",
            "flowchart LR",
            "    A[Сверка scope и production truth] --> B[Anonymous-first фокус-группа]",
            "    B --> C[Критический UI, Search, collections, legal]",
            "    C --> D[Интеграционный RC]",
            "    D --> E[Stabilization + sign-off]",
            "    E --> F[Freeze + cutoff 31 августа 18:00]",
            "    F --> G[1 сентября: atomic launch]",
            "    G --> H[D1–D10 soak и Telegraph decision]",
            "```",
            "",
            "## Канонические источники",
            "",
            "- [План production-релиза](../../features/static-site-pages/release-plan.md)",
            "- [Release autotest gates](../../features/static-site-pages/release-autotest-gates.md)",
            "- [Фокус-группа: продуктовый контракт](../../features/static-site-pages/focus-group.md)",
            "- [Фокус-группа: актуальный статус](../../features/static-site-pages/focus-group-release/status.md)",
            "- [Data ownership и 152-ФЗ gap](../../architecture/personalization-data-ownership.md)",
            "- [Email infrastructure](../../operations/email-delivery.md)",
            "- [Yandex dependency resilience](../../operations/yandex-dependency-resilience.md)",
            "- [Сводный follow-up audit — PR #323](https://github.com/onedayonemasterpiece/events-bot-new/pull/323)",
            "- [Prelaunch landing — PR #296](https://github.com/onedayonemasterpiece/events-bot-new/pull/296)",
            "",
            "## Правила правды",
            "",
            "1. `DONE` — только когда требуемый уровень evidence действительно достигнут.",
            "2. Документ, исследование или открытый PR не закрывают development/live пункт.",
            "3. Исторический candidate не закрывает проверку актуального `main` и target.",
            "4. Юридический пункт закрывается только фактическим публичным документом, "
            "реализацией соответствующего flow и правовой проверкой.",
            "5. Любой P0 `BLOCKED`, `NOT_STARTED`, `OWNER_GATE` или `VERIFY` сохраняет "
            "общий verdict `NO_GO`, если owner явно не изменил scope.",
            "",
        ]
    )
    return "\n".join(lines)


def render_checklist(data: dict[str, Any]) -> str:
    meta = data["meta"]
    items = data["item"]
    stream_order = meta["stream_order"]
    by_stream: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        by_stream[item["stream"]].append(item)

    status_rank = {
        "BLOCKED": 0,
        "OWNER_GATE": 1,
        "NOT_STARTED": 2,
        "IN_PROGRESS": 3,
        "VERIFY": 4,
        "DONE": 5,
        "DEFERRED": 6,
    }
    priority_rank = {"P0": 0, "P1": 1, "P2": 2}

    lines = [
        "<!-- GENERATED: edit checklist.toml, not this file. -->",
        f"# {meta['title']} — детальный checklist",
        "",
        f"> Срез {meta['updated_at']} · verdict **{meta['verdict']}** · "
        f"[сводка](README.md) · [kanban](KANBAN.md) · [обновление](UPDATE.md)",
        "",
        "Легенда: `E0` нет evidence · `E1` документ/решение · `E2` код/unit · "
        "`E3` интеграция/browser · `E4` hosted/candidate/device · `E5` production/live/soak.",
        "",
    ]

    for stream_index, stream in enumerate(stream_order, 1):
        stream_items = by_stream[stream]
        state, counts, done_p0, total_p0 = stream_summary(stream_items)
        lines.extend(
            [
                f'<a id="stream-{stream_index:02d}"></a>',
                f"## {stream}",
                "",
                f"**{state}** · P0 {done_p0}/{total_p0} · "
                f"blocked {counts['BLOCKED']} · verify {counts['VERIFY']} · "
                f"owner gate {counts['OWNER_GATE']}",
                "",
            ]
        )
        sorted_items = sorted(
            stream_items,
            key=lambda item: (
                priority_rank[item["priority"]],
                status_rank[item["status"]],
                item["id"],
            ),
        )
        for item in sorted_items:
            checkbox = "x" if item["status"] == "DONE" else " "
            lines.append(f'<a id="{item["id"].lower()}"></a>')
            lines.append(
                f"- [{checkbox}] **`{item['id']}` · {item['title']}**  "
            )
            lines.append(
                f"  `{item['priority']}` · `{item['phase']}` · "
                f"`{item['stage']}` · `{item['evidence']}` · "
                f"**{status_label(item['status'])}** · target `{item['target']}`"
            )
            lines.append(f"  - **Владелец:** {item['owner']}")
            lines.append(f"  - **Следующий шаг:** {item['next_action']}")
            lines.append(f"  - **Источник/evidence:** `{item['source']}`")
            if item.get("blocked_by"):
                deps = ", ".join(f"[`{dep}`](#{dep.lower()})" for dep in item["blocked_by"])
                lines.append(f"  - **Зависит от:** {deps}")
            if item.get("notes"):
                lines.append(f"  - **Примечание:** {item['notes']}")
            lines.append("")
    return "\n".join(lines)


def lane_for(item: dict[str, Any]) -> str:
    if item["status"] == "BLOCKED":
        return "blocked"
    if item["status"] == "OWNER_GATE":
        return "owner"
    if item["status"] == "VERIFY":
        return "verify"
    if item["status"] == "NOT_STARTED":
        return "queue"
    if item["status"] == "IN_PROGRESS":
        if item["stage"] in {"research", "decision", "design"}:
            return "discovery"
        if item["stage"] in {"development", "integration"}:
            return "build"
        return "verify"
    if item["status"] == "DONE":
        return "done"
    return "deferred"


def render_kanban(data: dict[str, Any]) -> str:
    meta = data["meta"]
    items = data["item"]
    by_id = {item["id"]: item for item in items}
    lanes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        lanes[lane_for(item)].append(item)

    lane_order = [
        ("blocked", "⛔ Заблокировано"),
        ("owner", "🧭 Требуется решение владельца"),
        ("discovery", "🔎 Исследование / решение / дизайн"),
        ("build", "🛠 Разработка / интеграция"),
        ("verify", "🧪 QA / hosted / live evidence"),
        ("queue", "○ Очередь"),
        ("done", "✅ Готово"),
        ("deferred", "⏸ Отложено"),
    ]
    priority_rank = {"P0": 0, "P1": 1, "P2": 2}

    lines = [
        "<!-- GENERATED: edit checklist.toml, not this file. -->",
        f"# {meta['title']} — kanban",
        "",
        f"> Срез {meta['updated_at']} · [сводка](README.md) · "
        f"[полный checklist](CHECKLIST.md)",
        "",
        "Kanban показывает движение deliverables, а не заменяет evidence. "
        "Верхний board — только критический path; ниже находится полный board.",
        "",
        "## Критический board",
        "",
    ]
    headline = [by_id[item_id] for item_id in meta.get("headline_ids", [])]
    for key, title in lane_order:
        selected = [item for item in headline if lane_for(item) == key]
        if not selected:
            continue
        lines.append(f"### {title}")
        lines.append("")
        for item in selected:
            lines.append(
                f"- {item_link(item['id'])} **{item['priority']} · {item['evidence']}** — "
                f"{item['title']}  \n  _Далее:_ {item['next_action']}"
            )
        lines.append("")

    lines.extend(["## Полный board", ""])
    for key, title in lane_order:
        selected = sorted(
            lanes[key],
            key=lambda item: (priority_rank[item["priority"]], item["stream"], item["id"]),
        )
        p0_count = sum(item["priority"] == "P0" for item in selected)
        lines.append(f"<details{' open' if key in {'blocked', 'owner', 'build', 'verify'} else ''}>")
        lines.append(
            f"<summary><strong>{title}</strong> — {len(selected)} пунктов, P0: {p0_count}</summary>"
        )
        lines.append("")
        current_stream = None
        for item in selected:
            if item["stream"] != current_stream:
                if current_stream is not None:
                    lines.append("")
                current_stream = item["stream"]
                lines.append(f"**{current_stream}**")
                lines.append("")
            lines.append(
                f"- {item_link(item['id'])} `{item['priority']}` `{item['stage']}` "
                f"`{item['evidence']}` — {item['title']}"
            )
        lines.append("")
        lines.append("</details>")
        lines.append("")

    lines.extend(
        [
            "## Опциональный GitHub Project",
            "",
            "После 1–2 циклов обновления этот источник можно синхронизировать с бесплатным "
            "GitHub Project. Рекомендуемые поля: `Status`, `Priority`, `Phase`, `Stage`, "
            "`Evidence`, `Target`, `Owner`, `Blocked by`, `Release`. До стандартизации "
            "Markdown/TOML остаются единственным источником правды, чтобы не возникло "
            "двух расходящихся досок.",
            "",
        ]
    )
    return "\n".join(lines)


def write_or_check(path: Path, content: str, check: bool) -> bool:
    content = content.rstrip() + "\n"
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current != content:
            print(f"OUT OF DATE: {path.relative_to(REPO_ROOT)}", file=sys.stderr)
            return False
        return True
    path.write_text(content, encoding="utf-8")
    print(f"generated {path.relative_to(REPO_ROOT)}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if generated views are stale")
    args = parser.parse_args()

    data = load()
    validate(data)

    outputs = {
        RELEASE_DIR / "README.md": render_readme(data),
        RELEASE_DIR / "CHECKLIST.md": render_checklist(data),
        RELEASE_DIR / "KANBAN.md": render_kanban(data),
    }
    ok = all(write_or_check(path, content, args.check) for path, content in outputs.items())
    if not ok:
        return 1

    counts = Counter(item["status"] for item in data["item"])
    print(
        "validated "
        f"{len(data['item'])} items; "
        f"DONE={counts['DONE']} BLOCKED={counts['BLOCKED']} "
        f"VERIFY={counts['VERIFY']} NOT_STARTED={counts['NOT_STARTED']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
