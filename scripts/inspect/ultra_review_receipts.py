"""Offline, non-mutating contract checks for saved Ultra pilot outputs.

These checks establish structural consistency only, never semantic correctness,
OCR accuracy or database acceptance. Inputs and native exports remain unchanged.
"""
from __future__ import annotations
import argparse,hashlib,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from source_parse_contract import SourceDisposition,LifecycleActionType

DISPOSITIONS={x.value for x in SourceDisposition}
ACTIONS={x.value for x in LifecycleActionType}
TASKS={
 'dvt_3a48ccdd43ad45489d4da555277a3209':('museum-v1',True,1,False),
 'dvt_e17a7bcc65684daa883efa0a52025066':('museum-v2',True,1,False),
 'dvt_dcf2ee0d20bb4061a404548cbb2f20ad':('agropark-partial-album',True,2,True),
 'dvt_fe20faaf91834e74a6566716529e59de':('text-nemotron-v1',False,0,False),
 'dvt_e9dc6ea21c1447e397f94152a02ccd3b':('text-big-pickle-v1',False,0,False),
}

def check_carrier(value,*,vision=False,expected_images=0,input_incomplete=False):
    errors=[]
    if not isinstance(value,dict):return ['object_required']
    d=value.get('disposition');e=value.get('events');a=value.get('lifecycle_actions')
    if d not in DISPOSITIONS:errors.append('unknown_disposition')
    if not isinstance(e,list):errors.append('events_array_required');e=[]
    if not isinstance(a,list):errors.append('actions_array_required');a=[]
    valid_counts={'EVENTS_FOUND':bool(e) and not a,'LIFECYCLE_ONLY':bool(a) and not e,
        'MIXED':bool(a) and bool(e),'CONFIRMED_NO_EVENT':not e and not a,'RETRY_REQUIRED':True}
    if d in valid_counts and not valid_counts[d]:errors.append('disposition_content_mismatch')
    if d=='CONFIRMED_NO_EVENT' and value.get('evidence_complete') is not True:
        errors.append('no_event_requires_complete_evidence')
    if input_incomplete and value.get('evidence_complete') is not False:
        errors.append('input_incompleteness_lost')
    for i,item in enumerate(a):
        if not isinstance(item,dict):errors.append(f'action[{i}].object_required');continue
        if item.get('kind') not in ACTIONS:errors.append(f'action[{i}].noncanonical_kind')
        if vision and not isinstance(item.get('support'),str):errors.append(f'action[{i}].support_string_required')
    if vision:
        if type(value.get('evidence_complete')) is not bool:errors.append('evidence_complete_boolean_required')
        blocks=value.get('ocr_blocks')
        if not isinstance(blocks,list):errors.append('ocr_blocks_array_required');blocks=[]
        if len(blocks)!=expected_images:errors.append('ocr_image_count_mismatch')
        ids=[]
        for i,b in enumerate(blocks):
            if not isinstance(b,dict):errors.append(f'ocr[{i}].object_required');continue
            if not isinstance(b.get('image_id'),str) or not b['image_id']:errors.append(f'ocr[{i}].image_id_required')
            else:ids.append(b['image_id'])
            if not isinstance(b.get('text'),str):errors.append(f'ocr[{i}].text_string_required')
            if type(b.get('unreadable')) is not bool:errors.append(f'ocr[{i}].unreadable_boolean_required')
        if len(ids)!=len(set(ids)):errors.append('duplicate_ocr_image_id')
    return errors

def review(root):
    rows=[]
    for task,(label,vision,n,incomplete) in TASKS.items():
        p=root/'native'/f'{task}.json'
        if not p.exists():rows.append({'trial':label,'status':'missing_export'});continue
        raw=p.read_bytes();doc=json.loads(raw);output=doc.get('parsed_output')
        assistant=[m['info'] for m in doc['messages_without_reasoning'] if m['info'].get('role')=='assistant']
        carriers=output.get('cases',[]) if isinstance(output,dict) and 'cases' in output else [output]
        checks=[]
        for c in carriers:
            checks.append({'case_id':c.get('id',label) if isinstance(c,dict) else label,
                'event_count':len(c.get('events',[])) if isinstance(c,dict) and isinstance(c.get('events'),list) else None,
                'errors':check_carrier(c,vision=vision,expected_images=n,input_incomplete=incomplete)})
        final=assistant[-1] if assistant else {};t=final.get('time') or {}
        first=(assistant[0].get('time') or {}).get('created') if assistant else None
        last=t.get('completed')
        rows.append({'trial':label,'task_id':task,'provider':final.get('providerID'),'model':final.get('modelID'),
            'assistant_messages':len(assistant),'elapsed_task_seconds':round((last-first)/1000,3) if first is not None and last is not None else None,
            'final_assistant_message_seconds':round((t['completed']-t['created'])/1000,3) if 'completed' in t and 'created' in t else None,
            'reported_token_totals_by_message':[(a.get('tokens') or {}).get('total') for a in assistant],
            'checks':checks,'native_export_sha256':hashlib.sha256(raw).hexdigest(),
            'claims_limited_to':'structural consistency; no semantic score or domain write acceptance'})
    return {'schema':'ultra-pilot-offline-review-v1','structural_checks_only':True,
        'text_prompt_contract_note':'Text v1 was a loose pilot without evidence_complete/native JSON schema. Missing evidence receipt is a production-readiness gap, not failure against an explicitly supplied field.',
        'timing_note':'Task/assistant wall time includes service/provider/permission effects. No pure inference latency or population benchmark is claimed.','trials':rows}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--save',action='store_true');a=p.parse_args()
    root=Path(a.root).resolve()
    if not root.is_relative_to('/home/dev/artifacts') or not (root/'.artifact.json').is_file():raise SystemExit('managed lab root required')
    result=review(root);data=(json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode()
    if a.save:
        name='offline-review-'+hashlib.sha256(data).hexdigest()[:16]+'.json';dest=root/'reviews';dest.mkdir(exist_ok=True)
        path=dest/name
        if path.exists():
            if path.read_bytes()!=data:raise RuntimeError('immutable review conflict')
        else:
            with path.open('xb') as f:f.write(data)
            path.chmod(0o600)
        result['saved_review']=str(path)
    print(json.dumps(result,ensure_ascii=False,indent=2))
