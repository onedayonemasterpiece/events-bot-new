"""Inspect native OpenCode lab tasks; approval is restricted to explicit public inputs.

No provider-policy/header modification, global permission change, production DB access,
or automated publishing. Approvals use OpenCode's documented one-request reply API.
"""
from __future__ import annotations
import argparse,hashlib,json,re,sys
from pathlib import Path
from datetime import datetime,timezone

LAB_PROJECT='handoff:src_5dd70fb837000a90095bdc3d'
ROOT=Path('/home/dev/artifacts/events-bot-new/20261005T073316Z-ultra-public-component-20261005')
TASK_INPUTS={
 'dvt_3a48ccdd43ad45489d4da555277a3209':['tg-ambermuseum-5626'],
 'dvt_e17a7bcc65684daa883efa0a52025066':['tg-ambermuseum-5626'],
 'dvt_dcf2ee0d20bb4061a404548cbb2f20ad':['tg-agropark39-2342','tg-agropark39-2343'],
 'dvt_fe20faaf91834e74a6566716529e59de':[],
 'dvt_e9dc6ea21c1447e397f94152a02ccd3b':[],
}

def allowed_paths(root:Path,cases:list[str])->set[Path]:
    paths=set()
    for case in cases:
        if not re.fullmatch(r'[A-Za-z0-9_-]+',case):raise ValueError('invalid case')
        p=root/'cases'/f'{case}.json'
        if p.is_symlink() or p.resolve()!=p:raise ValueError('symlink case')
        c=json.loads(p.read_text());paths.add(p)
        for image in c.get('images',[]):
            f=root/image['path']
            if not f.resolve().is_relative_to(root/'media') or f.is_symlink():raise ValueError('image path escaped')
            if hashlib.sha256(f.read_bytes()).hexdigest()!=image['sha256']:raise ValueError('image mismatch')
            paths.add(f.resolve())
    return paths

def approvable(row:dict,sid:str,allowed:set[Path])->bool:
    p=Path(row.get('metadata',{}).get('filepath','/'))
    return row.get('sessionID')==sid and row.get('permission') in ('external_directory','read') and p in allowed and p.resolve()==p and not p.is_symlink()

def parse_output(text:str):
    clean=re.sub(r'^```(?:json)?\s*|\s*```$','',text.strip())
    try:return json.loads(clean)
    except ValueError:return None

def main():
    ap=argparse.ArgumentParser();ap.add_argument('operation',choices=['inspect','approve','export']);ap.add_argument('--task',action='append');a=ap.parse_args()
    if not (ROOT/'.artifact.json').is_file():raise ValueError('managed root missing')
    sys.path.insert(0,'/home/dev/.local/libexec/openai-codex-mcp')
    from opencode_backend import OpenCodeBackend,TaskRegistry
    client=OpenCodeBackend();health=client._request_sync('GET','/global/health');registry=TaskRegistry();summaries=[]
    for task in a.task or TASK_INPUTS:
        if task not in TASK_INPUTS:raise ValueError('task not registered in lab manifest')
        rec=registry.get(task)
        if not rec or rec['project']!=LAB_PROJECT:raise ValueError('foreign task')
        sid=rec['backendSessionId'];cwd=rec['cwd'];allowed=allowed_paths(ROOT,TASK_INPUTS[task])
        pending=[x for x in client._request_sync('GET','/permission',directory=cwd) if x.get('sessionID')==sid]
        out={'task_id':task,'model':rec.get('selection'),'session_id':sid,'pending':[]}
        for row in pending:
            ok=approvable(row,sid,allowed)
            item={'id':row['id'],'permission':row.get('permission'),'is_exact_authorized_input':ok}
            if a.operation=='approve':
                if not ok:raise ValueError('unexpected permission: leave for operator review')
                item['approved_once']=client._request_sync('POST',f"/permission/{row['id']}/reply",directory=cwd,payload={'reply':'once'})
            out['pending'].append(item)
        if a.operation=='export':
            msgs=client._request_sync('GET',f'/session/{sid}/message',directory=cwd)
            safe=[];last=None;turns=[];tools=[]
            for msg in msgs:
                info=msg.get('info',{});parts=msg.get('parts',[])
                keep=[]
                for p in parts:
                    if p.get('type')=='text':keep.append({'type':'text','text':p.get('text','')})
                    elif p.get('type')=='tool':
                        st=p.get('state',{});t={'tool':p.get('tool'),'status':st.get('status'),'time':st.get('time'),'input':st.get('input'),'output':st.get('output'),'metadata':st.get('metadata')}
                        # File attachments remain in the private corpus; do not copy base64 into reports.
                        keep.append(t);tools.append({k:t[k] for k in ('tool','status','time')})
                safe.append({'info':{k:info.get(k) for k in ('id','role','time','modelID','providerID','cost','tokens','error','finish')},'parts':keep})
                if info.get('role')=='assistant':
                    turns.append({k:info.get(k) for k in ('id','modelID','providerID','time','cost','tokens','error','finish')})
                    texts=[p.get('text','') for p in parts if p.get('type')=='text' and not p.get('synthetic')]
                    if texts:last='\n'.join(texts)
            artifact={'observed_at':datetime.now(timezone.utc).isoformat(),'opencode_health':health,'task_id':task,'session_id':sid,'source_cases':TASK_INPUTS[task],'messages_without_reasoning':safe,'parsed_output':parse_output(last) if last else None,'raw_final_text':last,'scope':'public-source lab only; no DB materialization'}
            dest=ROOT/'native';dest.mkdir(exist_ok=True)
            path=dest/f'{task}.json';path.write_text(json.dumps(artifact,ensure_ascii=False,indent=2));path.chmod(0o600)
            out.update(assistant_turns=turns,tool_calls=tools,parsed_output=artifact['parsed_output'],artifact=str(path),artifact_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        summaries.append(out)
    print(json.dumps(summaries,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
