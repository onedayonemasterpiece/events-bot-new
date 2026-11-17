"""Analyze the saved Gemma study without a provider or a database.
python analyze.py results.jsonl > summary.json
"""
import hashlib
import json
import re
from pathlib import Path
import sys


def raw(verdict):
    return float(verdict['llm_semantic_score']) >= 0.72


def eligible(verdict):
    return raw(verdict) and not verdict['reject'] and verdict['similarity_class'] not in {
        'same_occurrence_group', 'duplicate_same_slot'
    }


def read(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line]


def verify_prompts(rows, captured):
    expected = {(r['anchor'], r['arm']): r for r in rows if r['fable17']=='frozen'}
    packets = {}
    for r in captured:
        key = (r['anchor'], r['arm'])
        assert key in expected and key not in packets
        assert hashlib.sha256(r['prompt'].encode()).hexdigest() == r['sha256'] == expected[key]['sha256']
        header, body = r['prompt'].split('<candidates>\n', 1)
        blocks = re.findall(r'<candidate id="(\d+)">\n(.*?)\n</candidate>',body,re.S)
        assert [int(peer) for peer,_ in blocks] == r['ids'] == expected[key]['ids']
        packets[key] = header, blocks
    assert set(packets) == set(expected)
    for (anchor,arm), (header,blocks) in packets.items():
        if arm == 'A':
            other_header, other = packets[anchor,'B']
            assert header == other_header
            assert sum(x == y for x,y in zip(blocks,other)) == 8
            assert len({peer for peer,_ in blocks} & {peer for peer,_ in other}) == 8


def count(items, field):
    return sum(row[field] is True for row in items)


def summarize(data):
    replies = {(r['anchor'], r['label']): r for r in data if r['fable17'] == 'reply'}
    frozen = {(r['anchor'], r['arm']): r for r in data if r['fable17'] == 'frozen'}
    actual = [r for r in data if r['fable17'] in ('reply', 'error', 'interrupted_sent')]
    assert all(r['sends'] in (0,1) for r in actual)
    assert len(replies) == sum(r['fable17']=='reply' for r in actual)
    for r in actual:
        f = frozen[r['anchor'], r['label'][0]]
        assert (f['sha256'], f['ids']) == (r['packet_sha256'], r['ids'])
    pairs = []
    def verdict(anchor, label, peer):
        response = replies.get((anchor,label), {})
        # Release candidate #1045 excludes all rows for a duplicated candidate.
        # The deployed parser may retain a last-written result for that ID.
        if str(peer) not in response.get('duplicate', ()):
            return response.get('scores', {}).get(str(peer))
        return None

    for (anchor, arm), first in frozen.items():
        if arm != 'A':
            continue
        bpeers = frozen[anchor, 'B']['ids']
        for pos, peer in enumerate(first['ids']):
            a = {l:v for l in ('A1', 'A2', 'A3') if (v:=verdict(anchor,l,peer)) is not None}
            shared = peer == bpeers[pos]
            b = verdict(anchor,'B',peer) if shared else None
            item = dict(anchor=anchor, peer=peer, slot=pos+1, n_A=len(a), shared=shared,
                        a_scores={k:v['llm_semantic_score'] for k,v in a.items()},
                        b_score=b['llm_semantic_score'] if b else None,
                        a_rejects={k:v['reject'] for k,v in a.items()},
                        b_reject=b['reject'] if b else None)
            values=list(item['a_scores'].values())
            item['A_score_changed']=len(set(values))>1 if len(values)>=2 else None
            item['B_score_changed']=item['b_score']!=values[0] if values and b else None
            item['B_score_delta']=round(item['b_score']-values[0],4) if values and b else None
            item['B_reject_changed']=item['b_reject']!=next(iter(item['a_rejects'].values())) if values and b else None
            for name, decision in [('raw',raw), ('eligible',eligible)]:
                values = [decision(v) for v in a.values()]
                bv = decision(b) if b else None
                majority = None
                if len(values) >= 2 and 2*sum(values) != len(values):
                    majority = 2*sum(values) > len(values)
                item[name+'_A_votes'] = {k:decision(v) for k,v in a.items()}
                item[name+'_B_vote'] = bv
                item[name+'_A_flip'] = len(set(values)) > 1 if len(a) >= 2 else None
                item[name+'_B_vs_A1_flip'] = bv != decision(a['A1']) if b and 'A1' in a else None
                item[name+'_B_vs_first_available_A_flip'] = bv != values[0] if b and values else None
                item[name+'_B_vs_A_majority_flip'] = bv != majority if bv is not None and majority is not None else None
                item[name+'_B_unseen_on_A'] = bv not in values if bv is not None and len(a)>=2 else None
            pairs.append(item)

    counts = {}
    for indicator in ('A_score_changed','B_score_changed','B_reject_changed'):
        counts[indicator] = [count(pairs,indicator),sum(r[indicator] is not None for r in pairs)]
    for name in ('raw', 'eligible'):
        for indicator in ('A_flip','B_vs_A1_flip','B_vs_first_available_A_flip','B_vs_A_majority_flip','B_unseen_on_A'):
            k = name+'_'+indicator
            counts[k] = [count(pairs,k),sum(row[k] is not None for row in pairs)]
        complete = [p for p in pairs if p['n_A']==3]
        counts[name+'_A_flip_all_three'] = [count(complete,name+'_A_flip'),len(complete)]
    return dict(sends=sum(r['sends'] for r in actual), replies=len(replies),
                pairs_by_A_response_count={str(n):sum(p['n_A']==n for p in pairs) for n in range(4)},
                physical_errors=[{'slot':r['slot'],'anchor':r['anchor'],'label':r['label'],'code':r.get('status_code'),
                                  'type':r.get('error_type')} for r in actual if r['fable17']=='error' and r['sends']],
                sent_without_response=[{'slot':r['slot'],'anchor':r['anchor'],'label':r['label']} for r in actual if r['fable17']=='interrupted_sent'],
                pre_send_blocks=sum(r['fable17']=='error' and not r['sends'] for r in actual),
                judged_unique_native=sum(len(r['scores']) for r in replies.values()),
                judged_unambiguous=sum(sum(str(p) not in r['duplicate'] for p in r['scores']) for r in replies.values()),
                missing=sum(len(r['missing']) for r in replies.values()),
                unexpected=sum(len(r['unexpected']) for r in replies.values()),
                duplicate=sum(len(r['duplicate']) for r in replies.values()),
                counts=counts,pairs=pairs)


if __name__ == '__main__':
    data = read(sys.argv[1])
    if len(sys.argv)>2:
        verify_prompts(data, json.loads(Path(sys.argv[2]).read_text()))
    print(json.dumps(summarize(data),ensure_ascii=False,indent=2))

