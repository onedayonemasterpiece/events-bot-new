"""Count candidate-pair routing on the two strict reader replays plus the first film packet.

Usage: python3 compare-triggers.py path/to/review-2-followup
Reads pinned evidence from #1046. Does not call a model. Python standard library only.
"""
from collections import Counter
import json
from pathlib import Path
import sys

base = Path(sys.argv[1])
records = [json.loads(line) for line in (base / 'combined.jsonl').read_text().splitlines()]
reader = json.loads((base / 'vk-reader-evidence.json').read_text())
# The original and Fable annotations enumerate positives, not an exhaustive list of negatives.
# Source: Fable fable_analysis.py (35b1ab110714d6b08ccbcccec7b7c25fc056ec1c).
marked = {11010: {10938, 10939, 10940, 10976}, 11068: {10883, 10443},
          10580: {10508, 10883, 11068, 11021, 10443, 10398}}
data = {}
for anchor, packets in reader['packets'].items():
    anchor = int(anchor)
    expected = {peer for peer, _ in reader['frozen_top20'][str(anchor)]}
    judged = {}
    for packet in packets:
        for peer, raw in packet['judged'].items():
            if int(peer) in packet['ids']:
                assert int(peer) not in judged
                judged[int(peer)] = raw
    assert len(expected) == 20 and set(judged) <= expected
    data[anchor] = {peer: judged.get(peer) for peer in sorted(expected)}

r = next(r for r in records if r['fable17'] == 'reply' and r['anchor'] == 11068 and r['label']=='A2')
data[11068] = {}
for peer in r['ids']:
    v = r['scores'].get(str(peer))
    data[11068][peer] = (
        [v['llm_semantic_score'], v['reject'], v['confidence'], v['similarity_class']]
        if v and str(peer) not in r['duplicate'] else None)

close_classes = {'thematically_close', 'highly_similar', 'identical_or_near_identical'}
blocked_classes = {'same_occurrence_group', 'duplicate_same_slot'}

def eligible(v):
    return v is not None and not v[1] and v[3] not in blocked_classes and v[0] >= 0.72

def route(kind, v):
    if v is None:
        return kind == 'unresolved'
    if kind == 'unresolved':
        return False
    score, reject, _, cls = v
    if kind == 'explicit_reject':
        return bool(reject)
    if reject or cls in blocked_classes:
        return False
    if kind == 'close_class_below':
        return cls in close_classes and score < 0.72
    if kind == 'near_below':
        return 0.68 <= score < 0.72
    if kind == 'wider_below':
        return 0.65 <= score < 0.72
    if kind == 'both_sides':
        return 0.68 <= score <= 0.76
    if kind == 'close_class_and_wider_below':
        return cls in close_classes and 0.65 <= score < 0.72
    raise ValueError(kind)

summary = {}
pair_data = []
for anchor, pairs in data.items():
    for peer, verdict in pairs.items():
        pair_data.append(dict(anchor=anchor, candidate=peer, gemma=verdict,
                              annotated_positive=peer in marked[anchor],
                              eligible=eligible(verdict)))
for kind in ('unresolved', 'near_below', 'wider_below', 'both_sides', 'close_class_below',
             'close_class_and_wider_below','explicit_reject'):
    hits = [r for r in pair_data if route(kind, r['gemma'])]
    summary[kind] = dict(
        pairs=len(hits),
        anchors=dict(sorted(Counter(r['anchor'] for r in hits).items())),
        human_positive=[[r['anchor'], r['candidate']] for r in hits if r['annotated_positive']],
        human_positive_not_admitted=[[r['anchor'],r['candidate']] for r in hits
                                     if r['annotated_positive'] and not r['eligible']],
        ids=[[r['anchor'],r['candidate']] for r in hits],
    )
warm_path=base.parent/'execution-evidence.json'
original=json.loads(warm_path.read_text())['warm_cache']
warm=original['captured_ids']['records']
defects=original['candidate_replay']['defects']
print(json.dumps({
    'source':'#1046 7ee773ceabed87d0bc9be30ce33bda80656115e5',
    'sample':'50 directed pairs. One frozen ten for 11068. Two twenties for 10580 and 11010. No B packets or historical days mixed.',
    'score_tuple':['score','reject','confidence','class'],
    'n_pairs':len(pair_data),
    'n_unambiguous':sum(r['gemma'] is not None for r in pair_data),
    'n_human_positive_present':sum(r['annotated_positive'] for r in pair_data),
    'n_human_positive_admitted':sum(r['annotated_positive'] and r['eligible'] for r in pair_data),
    'historical_32': {
        'time':original['captured_ids']['observed_at'],
        'anchors':len(warm),'pairs':sum(len(d['candidates']) for d in warm.values()),
        'missing_pairs':sum(len(set(d['candidates']) - set(d['answered'])) for d in warm.values()),
        'affected_anchors':len(defects),'repair_packets':sum(len(row['requested']) for row in defects.values()),
        'interpretation':'Five existing ten-based repair packets across four anchors, not five additional calls observed in production.'},
    'rules':summary,'pair_data':pair_data
},ensure_ascii=False,indent=2))
