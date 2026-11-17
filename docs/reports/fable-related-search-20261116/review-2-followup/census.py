"""Run from a repository checkout. Usage: python3 census.py /path/to/extracted/snapshot

Reuse Fable's published catalogue, title grouping and confirmed-family calculation.
Optional second argument: retained strict related-graph JSON for exposure counts.
No network, database or model access.
"""
import contextlib
import io
import json
import subprocess
import sys
from pathlib import Path

def main():
    root = Path.cwd()
    snapshot = Path(sys.argv[1]).resolve()
    graph = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else {}
    fname = "docs/reports/fable-related-search-20261116/fable_analysis.py"
    commit = "35b1ab110714d6b08ccbcccec7b7c25fc056ec1c"
    source = subprocess.check_output(["git", "show", f"{commit}:{fname}"])
    namespace = {"__file__": str(root / fname), "__name__": "fable_snapshot"}
    sys.argv = [fname, str(snapshot)]
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(source, str(root / fname), "exec"), namespace)
    groups = namespace["groups"]
    families = namespace["FAM_C"]
    slots = namespace["GS"]
    texts = namespace["T"]
    nodes = graph.get("related", {})
    result = []
    for members in groups.values():
        components = {}
        for i in members:
            components.setdefault(families[i], []).append(i)
        pairs = [(i,j) for i in members for j in members if families[i] != families[j]]
        shared_slots = sorted((i,j) for i,j in pairs if i < j and all(slots[str(i)][:2]) and slots[str(i)][:2] == slots[str(j)][:2])
        strict = []
        for i,j in pairs:
            node = nodes.get(str(i), {})
            if node.get("strict_verified") and j in node.get("pure_related", []):
                strict.append((i,j))
        result.append({
            "title": texts[min(members)]["title"],
            "components": sorted(sorted(ids) for ids in components.values()),
            "same_slot_pairs": shared_slots, "strict_pairs": strict,
        })
    counts = {"groups":len(result), "cards":sum(len(ids) for r in result for ids in r["components"]),
              "strict_groups":sum(bool(r["strict_pairs"]) for r in result),
              "same_slot_groups":sum(bool(r["same_slot_pairs"]) for r in result)}
    assert counts["groups"] == 36 and counts["cards"] == 135
    print(json.dumps({"graph_at":graph.get("generated_at"),"counts":counts, "groups":result},ensure_ascii=False))

if __name__ == "__main__":
    main()
