"""Replay the November 16 related-search snapshot using the repository's own ranking code."""
import argparse
import contextlib
import copy
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

SOURCE = "site/scripts/export-production-preview-data.py"
BASE = "d112e62d71043b13570cab420610d973412af0e7"
PR = "0808b35b9737e0fca2b9e1220488f6a5078b5086"
EVIDENCE = "docs/reports/related-vector-recall-evidence-20261116.json"
ROOT = Path(__file__).resolve().parents[3]


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def raw_comparison(bundle, rows):
    with tempfile.TemporaryDirectory() as temp:
        paths = [Path(temp) / name for name in ("production.jsonl", "experiment.jsonl")]
        for path, key in zip(paths, ("old", "U"), strict=True):
            with path.open("w", encoding="utf-8") as stream:
                for row in rows:
                    stream.write(json.dumps({"event_id": row["id"], "embedding": row[key]}) + "\n")
        job = subprocess.run(
            [sys.executable, str(ROOT / "scripts/compare_related_vectors.py"),
             *map(str, paths), "--evidence", str(ROOT / EVIDENCE),
             "--catalog-ids", str(bundle / "catalog-ids.json")],
            check=False,
        )
        return job.returncode


def load_exporter(ref, label):
    path = ROOT / SOURCE
    source = subprocess.check_output(["git", "-C", str(ROOT), "show", f"{ref}:{SOURCE}"])
    spec = importlib.util.spec_from_file_location(label, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[label] = module
    exec(compile(source, str(path), "exec"), module.__dict__)
    return module


def graph_comparison(bundle, rows):
    import numpy as np
    sys.path[:0] = [str(ROOT), str(ROOT / "site/scripts")]
    previous = load_exporter(BASE, "snapshot_previous")
    proposed = load_exporter(PR, "snapshot_proposed")
    facts = load_json(bundle / "metadata.json")
    times = load_json(bundle / "graph_sort.json")
    manifest = load_json(bundle / "catalog-ids.json")
    events = [r for r in facts["rows"] if r["active"] and r["lifecycle_status"] == "active"
              and (r["end_date"] or r["start_date"]) >= facts["as_of"]]
    events.sort(key=lambda r: (times[str(r["id"])][0], times[str(r["id"])][1] or "23:59", r["id"]))
    for event in events:
        event["start_time"], _, event["display_time"] = proposed.split_time(times[str(event["id"])][1])
    proposed.normalize_linked_occurrences(events)
    by_id = {r["id"]: r for r in events}
    ids = sorted(by_id)
    vectors = {r["id"]: r for r in rows}
    if ids != sorted(manifest) or ids != sorted(vectors) or len(rows) != len(ids):
        raise ValueError("catalog, vector and eligible metadata membership differ")
    if any((vectors[i]["old_hash"], vectors[i]["input_hash"]) !=
           (by_id[i]["previous_hash"], by_id[i]["expected"]) for i in ids):
        raise ValueError("source hashes differ from the frozen metadata")
    days = {i: date.fromisoformat(by_id[i]["start_date"]).toordinal() for i in ids}
    family = {i: members[0] for i, members in proposed.related_programme_members(events).items()}
    labels = load_json(ROOT / EVIDENCE)["quality_expectations"]

    def unit(matrix):
        norm = np.linalg.norm(matrix, axis=1)
        if matrix.shape != (len(ids), 768) or not np.isfinite(matrix).all() or not (norm > 0).all():
            raise ValueError("invalid vector matrix")
        return matrix / norm[:, None]

    def neighbours(matrix):
        result = {}
        for anchor, scores in zip(ids, matrix @ matrix.T, strict=True):
            pairs = [(i, float(s)) for i, s in zip(ids, scores, strict=True) if i != anchor]
            pairs.sort(key=lambda p: (-p[1], abs(days[p[0]] - days[anchor]), p[0]))
            result[anchor] = [{"event_id": i, "vector_similarity": s} for i, s in pairs[:60]]
        return result

    def replay(module, candidates):
        def read_frozen_rpc(name, payload, **kwargs):
            assert name == module.COMPACT_RELATED_RPC and payload["p_match_count"] == 60
            return candidates[payload["p_anchor_event_id"]]
        module.personalization_supabase_request = read_frozen_rpc
        with contextlib.redirect_stdout(io.StringIO()):
            chains = module.build_pgvector_related_chain(copy.deepcopy(events), current_date=facts["as_of"])
        if {int(i) for i in chains} != set(ids):
            raise ValueError("incomplete graph")
        result = {}
        for group in ("cases", "fresh_holdouts"):
            exact, represented, details = 0, 0, []
            for case in labels[group]:
                anchor = case["anchor"]
                peers = [v["event_id"] for v in chains[str(anchor)]]
                for peer in case["preferred"]:
                    pos = peers.index(peer) + 1 if peer in peers else None
                    rep = next(((n, i) for n, i in enumerate(peers, 1)
                                if family.get(i, i) == family.get(peer, peer)), None)
                    exact += pos is not None and pos <= 20
                    represented += rep is not None and rep[0] <= 20
                    details.append({"anchor": anchor, "peer": peer, "rank": pos, "first_programme": rep})
            result[group] = {"exact_at_20": exact, "programme_at_20": represented, "pairs": details}
        return result

    old, new = (unit(np.asarray([vectors[i][key] for i in ids], dtype=np.float64)) for key in ("old", "U"))
    old_top = neighbours(old)
    print(json.dumps({
        "events": len(ids), "previous": replay(previous, old_top),
        "pr": replay(proposed, old_top), "experimental": replay(proposed, neighbours(new)),
        "equal_mix": replay(proposed, neighbours(unit(old + new))),
    }, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("raw", "graph"))
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    with (args.bundle / "vectors.jsonl").open(encoding="utf-8") as stream:
        rows = [json.loads(line) for line in stream if line.strip()]
    return raw_comparison(args.bundle, rows) if args.stage == "raw" else graph_comparison(args.bundle, rows)


if __name__ == "__main__":
    raise SystemExit(main())
