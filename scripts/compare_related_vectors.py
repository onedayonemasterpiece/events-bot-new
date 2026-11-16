"""Compare two offline related_v1 vector exports against the same owner examples."""

import argparse
import json
import math
import sys
from collections import Counter
from functools import lru_cache
from itertools import product
from pathlib import Path

EVIDENCE = Path(__file__).resolve().parents[1] / "docs/reports/related-vector-recall-evidence-20261116.json"


def unique_keys(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def read_vectors(path):
    """JSON {id: [coordinates]} or .jsonl rows with event_id and embedding."""
    vectors = {}
    with path.open(encoding="utf-8") as stream:
        if path.suffix.lower() == ".jsonl":
            rows = (json.loads(line, object_pairs_hook=unique_keys) for line in stream if line.strip())
            entries = ((row["event_id"], row["embedding"]) for row in rows)
        else:
            data = json.load(stream, object_pairs_hook=unique_keys)
            if not isinstance(data, dict):
                raise ValueError(f"{path}: expected an object keyed by event ID")
            entries = data.items()
        seen = set()
        for raw_id, vector in entries:
            if not str(raw_id).isascii() or not str(raw_id).isdigit() or int(raw_id) <= 0:
                raise ValueError(f"{path}: invalid event ID {raw_id!r}")
            event_id = int(raw_id)
            if event_id in seen:
                raise ValueError(f"{path}: duplicate event {event_id}")
            seen.add(event_id)
            if not isinstance(vector, list) or len(vector) != 768 or any(
                type(x) not in (int, float) or not math.isfinite(x) for x in vector
            ):
                raise ValueError(f"{path}: event {event_id} requires 768 finite numbers")
            norm = math.hypot(*vector)
            if not norm or not math.isfinite(norm):
                raise ValueError(f"{path}: event {event_id} has invalid norm")
            vectors[event_id] = [x / norm for x in vector]
    return vectors


def triples(cases):
    return [
        (row["anchor"], pos, alt)
        for row in cases
        for pos, alt in product(row["preferred"], row["alternatives"])
    ]


def compare(expected, baseline, candidate, catalog_ids=None):
    original_pool = set(expected["fixed91"])
    cutoff = expected["rank_cutoff"]
    critical = set(triples(expected["critical_order"]))
    required_hits = {(row["anchor"], peer) for row in expected["required_full_hits"] for peer in row["preferred"]}
    fresh = set(triples(expected["fresh_holdouts"]))
    preferences = triples(expected["cases"]) + sorted(fresh)
    identity = expected["identity_control"]
    watched = {(w["anchor"], w["peer"]): w for w in expected["rank_watch"]}
    exports = [read_vectors(path) for path in (baseline, candidate)]
    pool = set(exports[0])
    if pool != exports[1].keys():
        raise ValueError(f"export ID sets differ: baseline-only {sorted(pool - exports[1].keys())}, "
                         f"candidate-only {sorted(exports[1].keys() - pool)}")
    if catalog_ids is not None:
        ids = json.loads(catalog_ids.read_text(encoding="utf-8"))
        if not isinstance(ids, list) or any(type(i) != int or i <= 0 for i in ids) or len(set(ids)) != len(ids):
            raise ValueError("catalog IDs must be an independent JSON list of unique positive integers")
        if set(ids) != pool:
            raise ValueError(f"catalog membership mismatch: missing {sorted(set(ids) - pool)}, extra {sorted(pool - set(ids))}")
    required = original_pool | {event for triple in preferences for event in triple}
    if missing := required - pool:
        raise ValueError(f"missing examples {sorted(missing)}")
    verified = catalog_ids is not None and bool(pool - required)
    # Every peer competes with the same weight. Self matches never contribute.
    @lru_cache(maxsize=None)
    def cosine(export, anchor, peer):
        vectors = exports[export]
        return math.fsum(x * y for x, y in zip(vectors[anchor], vectors[peer], strict=True))

    reasons = []

    def rank_pool(peers, fixed):
        scope = "fixed91" if fixed else "catalog"
        ranks, watched_ranks, outside_cutoff = [], [], []
        totals = {group: {"baseline": 0, "candidate": 0, "total": 0} for group in ("known", "fresh")}
        cases = list(expected["cases"])
        if not fixed:
            cases.extend(expected["fresh_holdouts"])
        cases.append(dict(anchor=identity["anchor"], preferred=[],
                          alternatives=identity["same_production"] + identity["other_session"]))
        for case in cases:
            anchor = case["anchor"]
            positions, ties = [], []
            for export in (0, 1):
                scored = sorted(((cosine(export, anchor, p), p) for p in peers if p != anchor),
                                key=lambda row: (-row[0], row[1]))
                positions.append({p: i for i, (_, p) in enumerate(scored, 1)})
                counts = Counter(score for score, _ in scored)
                ties.append({p: counts[score] - 1 for score, p in scored})
            for peer in case["preferred"] + case["alternatives"]:
                before, after = (position[peer] for position in positions)
                positive = peer in case["preferred"]
                if positive:
                    group = "fresh" if case in expected["fresh_holdouts"] else "known"
                    counts = totals[group]
                    counts["baseline"] += before <= cutoff
                    counts["candidate"] += after <= cutoff
                    counts["total"] += 1
                    if after > cutoff:
                        outside_cutoff.append(dict(anchor=anchor, peer=peer, group=group,
                                                   baseline=before, candidate=after))
                        if fixed:
                            reasons.append([scope, "positive_outside_cutoff", anchor, peer])
                        elif before <= cutoff:
                            reasons.append([scope, "lost_positive_at_cutoff", anchor, peer])
                        elif (anchor, peer) in required_hits:
                            reasons.append([scope, "required_positive_outside_cutoff", anchor, peer])
                if fixed and (measurement := watched.get((anchor, peer))):
                    watched_ranks.append({
                        **measurement, "baseline": before, "candidate": after,
                        "delta_vs_fixed_v2": after - measurement["production"],
                        "delta_vs_rejected_draft": after - measurement["summary_draft"],
                    })
                    if "better_than_draft_rank_lt" in measurement and after >= measurement["better_than_draft_rank_lt"]:
                        reasons.append([scope, "watched_rank", anchor, peer])
                ranks.append(dict(anchor=anchor, peer=peer, baseline=before, candidate=after,
                                  rank_change=after - before, expected_positive=positive,
                                  identity_control=anchor == identity["anchor"],
                                  baseline_ties=ties[0][peer], candidate_ties=ties[1][peer]))
        return dict(count=len(peers), hits_at_cutoff=totals["known"], fresh_at_cutoff=totals["fresh"],
                    outside_cutoff=outside_cutoff, watched_ranks=watched_ranks, ranks=ranks)

    fixed_report = rank_pool(original_pool, fixed=True)
    catalog_report = rank_pool(pool, fixed=False)
    comparisons, satisfied, holdout_wins = [], [0, 0], [0, 0]
    for anchor, pos, alt in preferences:
        key = (anchor, pos, alt)
        margins = [cosine(i, anchor, pos) - cosine(i, anchor, alt) for i in (0, 1)]
        before, after = [m > 0 for m in margins]
        totals = holdout_wins if key in fresh else satisfied
        totals[0] += before
        totals[1] += after
        if not after and (before or key in critical or key in fresh):
            reasons.append(["preference", anchor, pos, alt])
        comparisons.append(dict(anchor=anchor, preferred=pos, alternative=alt, critical=key in critical,
                                fresh=key in fresh, baseline_margin=margins[0], candidate_margin=margins[1],
                                baseline_wins=before, candidate_wins=after))
    status = "pass" if verified else "limited_diagnostic"
    if reasons:
        status = "no_go"
    return dict(status=status, catalog_membership="manifest_match" if catalog_ids is not None else "unverified",
                competitors_beyond_examples=len(pool - required),
                reasons=reasons, cutoff=cutoff, fixed91=fixed_report, supplied_catalog=catalog_report,
                preferences=dict(baseline=satisfied[0], candidate=satisfied[1], total=len(preferences) - len(fresh)),
                holdouts=dict(baseline=holdout_wins[0], candidate=holdout_wins[1], total=len(fresh)),
                comparisons=comparisons)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help='JSON {"ID":[768 floats]} or .jsonl {"event_id":ID,"embedding":[...]}')
    parser.add_argument("candidate", type=Path, help="same event snapshot and vector model as baseline")
    parser.add_argument("--evidence", type=Path, default=EVIDENCE)
    parser.add_argument("--catalog-ids", type=Path,
                        help="independently captured list of every eligible catalog ID on this snapshot; "
                             "required for a pass, omission still reports both pools")
    args = parser.parse_args()
    try:
        expected = json.loads(args.evidence.read_text(encoding="utf-8"), object_pairs_hook=unique_keys)["quality_expectations"]
        report = compare(expected, args.baseline, args.candidate, args.catalog_ids)
    except (OSError, ValueError, TypeError, KeyError, OverflowError) as exc:
        print(json.dumps(dict(status="invalid_input", error=str(exc)), ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    return {"pass": 0, "no_go": 1, "limited_diagnostic": 3}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
