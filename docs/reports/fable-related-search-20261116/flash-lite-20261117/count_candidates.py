"""Recompute potential second-opinion volume from recorded current cache packets (stdlib)."""
import json
import sys
from pathlib import Path

def count(census):
    strong = {"identical_or_near_identical","highly_similar","thematically_close"}
    metrics={k:{"pairs":0,"anchors":0,"max_pairs_on_one_anchor":0} for k in (
        "scored","missing_in_saved_packet","non_reject_in_055_072","non_reject_in_065_072",
        "strong_class_below_threshold","strong_class_in_065_072","weak_class_above_threshold",
        "missing_or_strong_class_below_threshold")}
    complete = {"anchors_with_twenty_scored": 0, "at_most_one_admitted": 0,
                "anchors_with_band": 0, "pairs_in_band": 0}
    for rows in census["anchors"].values():
        if len(rows) == 20 and all(row[1] is not None for row in rows):
            complete["anchors_with_twenty_scored"] += 1
            eligible = [r for r in rows if not r[3] and census["classes"][r[2]] not in
                        {"same_occurrence_group", "duplicate_same_slot"}]
            if sum(r[1] >= .72 for r in eligible) <= 1:
                complete["at_most_one_admitted"] += 1
                band = sum(.65 <= r[1] < .72 for r in eligible)
                complete["anchors_with_band"] += band > 0
                complete["pairs_in_band"] += band
        flags={k:0 for k in metrics}
        for _,score,cl,refused in rows:
            if score is None:
                flags["missing_in_saved_packet"]+=1
                flags["missing_or_strong_class_below_threshold"]+=1
                continue
            cls=census["classes"][cl]
            low=not refused and cls in strong and score<.72
            flags["scored"]+=1
            flags["non_reject_in_055_072"]+=not refused and .55<=score<.72
            flags["non_reject_in_065_072"]+=not refused and .65<=score<.72
            flags["strong_class_below_threshold"]+=low
            flags["strong_class_in_065_072"]+=low and score>=.65
            flags["weak_class_above_threshold"]+=not refused and score>=.72 and cls in {"weak_or_adjacent_related","different_topic"}
            flags["missing_or_strong_class_below_threshold"]+=low
        for key,n in flags.items():
            metrics[key]["pairs"]+=n
            metrics[key]["anchors"]+=n>0
            metrics[key]["max_pairs_on_one_anchor"]=max(metrics[key]["max_pairs_on_one_anchor"],n)
    return {"all_saved_packets": metrics, "fully_scored_anchors": complete}

if __name__=="__main__":
    print(json.dumps(count(json.loads(Path(sys.argv[1]).read_text())),indent=2))
