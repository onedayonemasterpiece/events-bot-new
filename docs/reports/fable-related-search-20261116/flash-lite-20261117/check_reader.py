"""Verify archived VK reader projections against Gemma, Lite and exact public URLs."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
PARENT = HERE.parent
ANCHOR = 11010


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def lite_grades(filename, anchor):
    replies = [
        row for row in map(json.loads, (HERE / filename).read_text().splitlines())
        if row["kind"] == "reply" and row["anchor"] == anchor
    ]
    assert len(replies) == 1
    rows = json.loads(replies[0]["raw"])["ranked"]
    return {
        row["event_id"]: (row["llm_semantic_score"], row["reject"],
                          row["confidence"], row["similarity_class"])
        for row in rows
    }


previous = load(PARENT / "review-2-followup" / "vk-reader-evidence.json")
measured = load(HERE / "reader-impact.json")
vk = load(PARENT / "execution-evidence.json")["vk_projection"]["records"]
receipt, = (record for record in vk if "public_at" in record)
assert receipt["public_at"] == measured["public_receipt_utc"]
assert previous["reader"]["versions"]["presentation"] == measured["vk_code_sha256"]
for peer_id in (10938, 10939, 10940, 10976, 10883):
    fields = measured["facts"][str(peer_id)]
    url, public, *_ = receipt["peers"][str(peer_id)]
    assert fields["eligible"] and public and fields["url"] == url
assert measured["facts"]["9950"]["url"] is None
similarity = dict(previous["frozen_top20"][str(ANCHOR)])
base = {
    int(event_id): grade
    for packet in previous["packets"][str(ANCHOR)]
    for event_id, grade in packet["judged"].items() if int(event_id) in packet["ids"]
}
assert len(similarity) == 20 and set(similarity) - base.keys() == {10939}
single = lite_grades("lite-singleton-responses.jsonl", ANCHOR)
assert set(single) == {10939}
panel = lite_grades("lite-responses.jsonl", ANCHOR)
cases = {
    "gemma": base,
    "technical_singleton": base | single,
    "technical_and_two_near_pairs": base | single | {i: panel[i] for i in (10976, 10940)},
}


def strong_ids(grades):
    ranked = []
    for event_id, (score, reject, confidence, category) in grades.items():
        if reject or score < 0.72 or category in {"duplicate_same_slot", "same_occurrence_group"}:
            continue
        sim = similarity[event_id]
        weight = round(0.82 * score + 0.12 * sim + 0.06 * confidence, 4)
        ranked.append((-weight, -sim, event_id))
    return [i for _, _, i in sorted(ranked)]


def check_projection(name, anchor, candidates):
    reported = measured["result"][name]
    assert reported["anchor"] == anchor and reported["strong_ids"] == candidates
    visible = []
    for event_id in candidates:
        fact = measured["facts"][str(event_id)]
        if not fact["eligible"] or not fact["url"]:
            continue
        url, public, *_ = receipt["peers"][str(event_id)]
        assert url == fact["url"]
        if public:
            visible.append((event_id, url))
    assert visible == [(row["id"], row["url"]) for row in reported["reader"]]
    print(f"{name}: {[i for i, _ in visible]}")


for name, grades in cases.items():
    check_projection(name, ANCHOR, strong_ids(grades))
pi = lite_grades("lite-responses.jsonl", 11068)[10883]
assert pi[0] >= 0.72 and not pi[1]
check_projection("pi_only_visibility_probe", 11068, [10883])
