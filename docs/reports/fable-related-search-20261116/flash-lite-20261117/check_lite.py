"""Offline check of frozen labels, inputs, scorer outputs and selective strategies; Python 3."""
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

root=Path(__file__).resolve().parent
def read(name):
    return (root/name).read_bytes()
def sha(data):
    return hashlib.sha256(data).hexdigest()

def show(row):
    if row is None:
        return None
    return row['llm_semantic_score'] >= .72 and not row.get('reject') and row.get('similarity_class') not in ('same_occurrence_group','duplicate_same_slot')

def series(prefix):
    design_bytes=read(f'{prefix}-design.json')
    packet_bytes=read(f'{prefix}-packets.json')
    design=json.loads(design_bytes)
    packets=json.loads(packet_bytes)
    records=[json.loads(l) for l in read(f'{prefix}-responses.jsonl').splitlines() if l.strip()]
    head=records[0]
    assert head['kind']=='start' and records[-1]['kind']=='finished'
    assert head['design_file_sha256']==sha(design_bytes)
    assert head['packets_file_sha256']==sha(packet_bytes)
    assert head['utc']>design['frozen_at']
    assert head['config']==design['generation_config']
    by_anchor={r['anchor']:r for r in records if r['kind'] in ('reply','error')}
    sent=[r for r in records if r['kind']=='sent']
    assert len(sent)<=design['planned_sends'] and len({r['anchor'] for r in sent})==len(sent)
    assert all('gemini-3.5-flash-lite' in (r['provider_model_name'] or r['provider_model']) for r in sent)
    verdicts={}
    coverage=[]
    totals=Counter()
    for p in packets:
        aid=p['anchor'];single=by_anchor[aid]
        assert sha(p['prompt'].encode())==p['sha256']
        if single['kind']=='error':
            coverage.append(dict(anchor=aid,error=single.get('code') or single['error_type']))
            continue
        assert single['packet_sha256']==p['sha256']
        raw=json.loads(single['raw'])['ranked']
        expected=set(p['ordered_ids'])
        assert [r['candidate'] for r in design['labels'] if r['anchor']==aid]==p['ordered_ids']
        assert [int(i) for i in re.findall(r'<candidate id="(\d+)">',p['prompt'])]==p['ordered_ids']
        counts=Counter(r['event_id'] for r in raw)
        good=[]
        for r in raw:
            i=r['event_id']
            if i not in expected or counts[i]!=1:continue
            assert type(r['reject']) is bool and 0 <= r['llm_semantic_score'] <= 1 and 0 <= r['confidence'] <= 1
            verdicts[(aid,i)]=r
            good.append(i)
        coverage.append(dict(anchor=aid,requested=len(expected),unique_target=len(good),duplicate=[i for i in expected if counts[i]>1],
                             extra=sorted(set(counts)-expected),missing=sorted(expected-set(counts))))
        totals.update({k:v for k,v in single['usage'].items() if type(v) is int})
    rows=[]
    for ref in design['labels']:
        a,c=ref['anchor'],ref['candidate']
        gemma=show(ref['gemma'])
        lite=verdicts.get((a,c))
        result=show(lite)
        rows.append(dict(anchor=a,candidate=c,role=ref['role'],expected=ref['expected'],gemma=gemma,
                         lite=result,lite_score=lite['llm_semantic_score'] if lite else None,
                         sent_packet_size=next(len(p['ordered_ids']) for p in packets if p['anchor']==a),
                         correct=(result==ref['expected']) if result is not None else None))
    counts=Counter(('missing' if x['correct'] is None else 'agree' if x['correct'] else 'disagree') for x in rows)
    obj=dict(sent=len(sent),coverage=coverage,usage=totals,labels=counts,cases=rows)
    return obj

def main():
    primary=series('lite')
    single=series('lite-singleton')
    sp=json.loads(read('lite-singleton-design.json'))
    assert sp['failed_gemma']['status_code']==503
    assert sp['packet_sha256']==sp['failed_gemma']['packet_sha256']
    assert {(r['anchor'],r['candidate']) for r in single['cases']}=={(11010,10939)}
    unique={(r['anchor'],r['candidate']) for r in primary['cases']+single['cases']}
    totals=Counter(primary['usage'])+Counter(single['usage'])
    print(json.dumps(dict(primary=primary,exact_singleton=single,unique_pairs=len(unique),
                          physical_sends=primary['sent']+single['sent'],total_usage=totals),indent=2,ensure_ascii=False))

if __name__=='__main__':
    main()
