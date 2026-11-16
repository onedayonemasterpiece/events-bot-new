"""Fable: независимый пересчёт по снимку 690 событий. Запуск: python fable_analysis.py <snapshot_dir>

Нужен NumPy и зависимости экспортёра (как для `replay.py graph`). Модель и база не вызываются.
"""
import contextlib
import importlib.util
import io
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
SOURCE = "site/scripts/export-production-preview-data.py"
PR = "0808b35b9737e0fca2b9e1220488f6a5078b5086"
S = Path(sys.argv[1])

# Исходная разметка (7 карточек + 2 holdout) и разметка Fable: 15 новых карточек,
# пары записаны по названиям и текстам до просмотра рангов.
ORIG = {10769: [10725, 10256], 11068: [10883, 10443], 11010: [10938, 10939, 10940, 10976],
        11011: [10441, 10648], 10648: [10441, 11011], 10800: [10860], 11074: [9938, 9762],
        7523: [10860], 9503: [10031]}
FABLE = {
    11081: [11079, 11080, 11082, 9179], 10556: [10555, 8870, 9762, 9580, 10163],
    10424: [11066, 10929, 10861, 11052, 10419, 10934, 10935], 10933: [11006, 10932, 10949, 11008],
    10966: [10329, 10509, 10964, 10965, 10610], 8624: [8623, 8625, 7236, 10504],
    10628: [10352, 10355, 10625, 10353, 10356, 10358], 10520: [11087, 11024, 10867, 11011, 10351],
    10561: [10558, 10557, 10851, 11035, 10383, 10798], 10739: [10431, 10931, 10544],
    10580: [10508, 10883, 11068, 11021, 10443, 10398], 10401: [10402, 10403, 10396, 10119],
    10616: [10615, 10803, 10459], 10977: [10936, 10938], 10612: [10421, 10930, 11079],
}

T = {d["id"]: d for d in map(json.loads, (S / "texts.jsonl").open(encoding="utf-8"))}
V = [json.loads(line) for line in (S / "vectors.jsonl").open(encoding="utf-8")]
GS = json.loads((S / "graph_sort.json").read_text())
ids = [v["id"] for v in V]
ix = {i: n for n, i in enumerate(ids)}


def unit(m):
    return m / np.linalg.norm(m, axis=1, keepdims=True)


OLD, U = unit(np.array([v["old"] for v in V])), unit(np.array([v["U"] for v in V]))
SIMS = {"v2": OLD @ OLD.T, "U": U @ U.T, "mix": (m := unit(OLD + U)) @ m.T}


def confirmed_families():
    sys.path[:0] = [str(ROOT), str(ROOT / "site/scripts")]
    src = subprocess.check_output(["git", "-C", str(ROOT), "show", f"{PR}:{SOURCE}"])
    spec = importlib.util.spec_from_file_location("fable_exporter", ROOT / SOURCE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fable_exporter"] = mod
    exec(compile(src, str(ROOT / SOURCE), "exec"), mod.__dict__)
    events = json.loads((S / "metadata.json").read_text())["rows"]
    events.sort(key=lambda r: (GS[str(r["id"])][0], GS[str(r["id"])][1] or "23:59", r["id"]))
    for e in events:
        e["start_time"], _, e["display_time"] = mod.split_time(GS[str(e["id"])][1])
    with contextlib.redirect_stdout(io.StringIO()):
        mod.normalize_linked_occurrences(events)
        return {i: m[0] for i, m in mod.related_programme_members(events).items()}


def tkey(i):
    s = T[i]["title"].lower().replace("ё", "е")
    quoted = re.search(r"«(.+)»", s)
    return re.sub(r"[^a-zа-я0-9]+", "", quoted.group(1) if quoted else s)


parent = {i: i for i in ids}


def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


for i, head in confirmed_families().items():
    if i in parent and head in parent:
        parent[find(i)] = find(head)
FAM_C = {i: find(i) for i in ids}  # подтверждённые связи (как в сборщике PR #1027)
by_key = {}
for i in ids:
    by_key.setdefault(tkey(i), []).append(i)
for key, members in by_key.items():
    if len(key) >= 6:
        for j in members[1:]:
            parent[find(j)] = find(members[0])
FAM_X = {i: find(i) for i in ids}  # плюс совпадение ядра названия


def order(sim, a):
    return [ids[j] for j in np.argsort(-sim[ix[a]], kind="stable") if ids[j] != a]


def collapse(lst, a, fam):
    seen, out = {fam[a]}, []
    for i in lst:
        if fam[i] not in seen:
            seen.add(fam[i])
            out.append(i)
    return out


def prank(sim, a, p, fam):
    return next(n for n, i in enumerate(collapse(order(sim, a), a, fam), 1) if fam[i] == fam[p])


print("## 1. Место нужной программы в векторной выдаче (одна строка на программу)")
for fam_name, fam in (("подтверждённые связи", FAM_C), ("связи + название", FAM_X)):
    for set_name, labels in (("исходная разметка", ORIG), ("разметка Fable", FABLE)):
        pairs = [(a, p) for a, ps in labels.items() for p in ps]
        print(f"\n{fam_name} / {set_name}: пар {len(pairs)}")
        ranks = {n: [prank(s, a, p, fam) for a, p in pairs] for n, s in SIMS.items()}
        for n, r in ranks.items():
            print(f"  {n:4s} в первых 5: {sum(x <= 5 for x in r):2d}  10: {sum(x <= 10 for x in r):2d}"
                  f"  20: {sum(x <= 20 for x in r):2d}  40: {sum(x <= 40 for x in r):2d}  худшее место: {max(r)}")
        if fam is FAM_X:
            for (a, p), old, new in zip(pairs, ranks["v2"], ranks["mix"]):
                if (old <= 20) != (new <= 20):
                    print(f"    {'+' if new <= 20 else '-'} {a} «{T[a]['title'][:30]}» → {p} «{T[p]['title'][:30]}»: {old} → {new}")

print("\n## 2. Сколько разных программ в первых 20 кандидатах (v2, все 690 карточек)")
for name, fam in (("без отбора", None), ("программы вперёд, подтверждённые связи", FAM_C),
                  ("программы вперёд, связи + название", FAM_X)):
    counts = []
    for a in ids:
        lst = [i for i in order(SIMS["v2"], a) if FAM_C[i] != FAM_C[a]][:40]
        lst = collapse(lst, a, fam) if fam else lst
        counts.append(len({FAM_X[i] for i in lst[:20]}))
    print(f"  {name}: в среднем {np.mean(counts):.2f}; карточек с ≤10 программами: {sum(c <= 10 for c in counts)}")

print("\n## 3. Несвязанные карточки с одинаковым названием")
groups = {k: v for k, v in by_key.items() if len(k) >= 6 and len({FAM_C[i] for i in v}) > 1}
hit = sum(any(tkey(i) == tkey(a) and len(tkey(a)) >= 6
              for i in [i for i in order(SIMS["v2"], a) if FAM_C[i] != FAM_C[a]][:20]) for a in ids)
print(f"  групп: {len(groups)}, событий в них: {sum(map(len, groups.values()))}; "
      f"карточек, у которых такой двойник в первых 20: {hit} из {len(ids)}")
for key, members in sorted(groups.items(), key=lambda kv: -len(kv[1])):
    low = min(float(OLD[ix[x]] @ OLD[ix[y]]) for x in members for y in members if x != y)
    print(f"    {T[members[0]]['title'][:48]:48s} событий {len(members):2d}, мин. близость {low:.2f}")

print("\n## 4. Сохранённые оценки Gemma (4 карточки)")
hist = json.loads((S / "expected/historical-observations.json").read_text())["cases"]
band = lambda s: "≥0.72" if s >= 0.72 else "0.55–0.71" if s >= 0.55 else "<0.55"
print("  ", dict(Counter((v[2], band(v[0])) for c in hist.values() for v in c["judged"].values())))


def shown(a, fill, fam):
    judged = sorted(((v[0], int(i)) for i, v in hist[str(a)]["judged"].items() if not v[1]), key=lambda x: -x[0])
    lst = [i for s, i in judged if s >= 0.72] + ([i for s, i in judged if 0.55 <= s < 0.72] if fill else [])
    return collapse(lst, a, fam)[:5]


for a in map(int, hist):
    print(f"\n  {a} «{T[a]['title']}»")
    for label, peers in ORIG.items():
        if label == a:
            for p in peers:
                verdict = hist[str(a)]["judged"].get(str(p))
                print(f"    нужная пара {p} «{T[p]['title'][:40]}»: оценка Gemma {verdict[0] if verdict else 'не оценивалась'}")
    for name, fill, fam in (("сейчас", 0, FAM_C), ("с добором 0.55–0.71", 1, FAM_C), ("добор + название", 1, FAM_X)):
        print(f"    {name}: " + "; ".join(f"{T[i]['title'][:34]} ({hist[str(a)]['judged'][str(i)][0]})"
                                           for i in shown(a, fill, fam)))
