"""Bounded public Telegram album acquisition for the existing Ultra lab.

Run only via telegram-e2e-run; no sends, DB access, environment copies or model calls.
Original case packets are immutable. Derived albums have their own capture receipt.
"""
from __future__ import annotations
import argparse,asyncio,json,sys
from pathlib import Path
from datetime import datetime,timezone
from ultra_component_lab import root_path,save,sha


def observed_bounds(messages, group_id):
    group=[m for m in messages if str(getattr(m,'grouped_id',None))==group_id]
    if not group:return False,[]
    lo=min(m.id for m in group);hi=max(m.id for m in group)
    before=[m.id for m in messages if m.id<lo and str(getattr(m,'grouped_id',None))!=group_id]
    after=[m.id for m in messages if m.id>hi and str(getattr(m,'grouped_id',None))!=group_id]
    return bool(before and after),[max(before) if before else None,min(after) if after else None]

async def collect(args):
    root=root_path(args.root);repo=Path(__file__).resolve().parents[2]
    sys.path.insert(0,str(repo/'scripts'))
    import read_telegram_message as r
    r._load_dotenv_if_present=lambda *a,**k:None
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    cfg=r._load_config()
    kw={k:getattr(cfg,k) for k in ('device_model','system_version','app_version','lang_code','system_lang_code') if getattr(cfg,k)}
    client=TelegramClient(StringSession(cfg.session_string),cfg.api_id,cfg.api_hash,
        flood_sleep_threshold=0,request_retries=1,connection_retries=1,**kw)
    await client.connect();out=[]
    try:
        if not await client.is_user_authorized():raise RuntimeError('local credential needs renewal')
        for seed in args.seed:
            if '/' in seed or '..' in seed:raise ValueError('invalid seed')
            original=json.loads((root/'cases'/f'{seed}.json').read_text())
            channel=original['source_group'];group_id=original.get('grouped_id')
            if not group_id:raise ValueError('seed is not an album')
            target=root/'assembled'/f'{channel}-{group_id}.json'
            if target.exists():
                value=json.loads(target.read_text());out.append({'album':str(target),'reused':True,'messages':len(value['messages'])});continue
            msg_id=int(original['source_url'].rstrip('/').split('/')[-1]);lower=max(1,msg_id-24);upper=msg_id+24
            entity=await client.get_entity(channel)
            if not getattr(entity,'username',None):raise ValueError('public channel required')
            nearby=[m for m in await client.get_messages(entity,ids=list(range(lower,upper+1))) if m is not None]
            bounded,neighbors=observed_bounds(nearby,group_id)
            selected=sorted((m for m in nearby if str(m.grouped_id)==group_id),key=lambda x:x.id)
            rows=[];errors=[]
            for m in selected:
                row={'id':m.id,'text':m.message or '', 'source_url':f'https://t.me/{channel}/{m.id}',
                    'published_at':m.date.isoformat(),'edited_at':m.edit_date.isoformat() if m.edit_date else None,'images':[]}
                if m.photo:
                    p=root/'media'/f'tg-{channel}-{m.id}.jpg'
                    # Reuse immutable captured bytes when the source timestamp is unchanged.
                    cp=root/'cases'/f'tg-{channel}-{m.id}.json'
                    cached=json.loads(cp.read_text()) if cp.exists() else None
                    if p.exists() and (not cached or cached.get('edited_at')!=row['edited_at']):
                        p=root/'media'/f'tg-{channel}-{m.id}-assembled.jpg'
                    if not p.exists():
                        result=await asyncio.wait_for(client.download_media(m,file=str(p)),45)
                        if not result:errors.append(f'{m.id}:photo_unavailable');continue
                        p=Path(result);p.chmod(0o600)
                    data=p.read_bytes()
                    row['images'].append({'path':str(p.relative_to(root)),'sha256':sha(data),'bytes':len(data),'mime':'image/jpeg'})
                elif m.document:errors.append(f'{m.id}:document_not_captured')
                rows.append(row)
            if not bounded:errors.append('album_bounds_unconfirmed')
            value={'capture_kind':'live_telethon_bounded_album','observed_at':datetime.now(timezone.utc).isoformat(),
                'source_group':channel,'grouped_id':group_id,'seed':seed,'domain':original['domain'],
                'queried_ids':[lower,upper],'neighbor_ids':neighbors,'bounds_observed':bounded,
                'messages':rows,'evidence_errors':errors,'scope_note':'Observed snapshot only; later edits remain new source revisions.'}
            value['capture_hash']=sha(json.dumps(rows,ensure_ascii=False,sort_keys=True).encode())
            save(target,value)
            out.append({'album':str(target),'messages':len(rows),'message_ids':[m['id'] for m in rows],
                'caption_chars':sum(len(m['text']) for m in rows),'images':sum(len(m['images']) for m in rows),
                'bounds_observed':bounded,'neighbor_ids':neighbors,'evidence_errors':errors,'capture_hash':value['capture_hash']})
    finally:await client.disconnect()
    print(json.dumps(out,ensure_ascii=False,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--seed',action='append',required=True)
    a=p.parse_args()
    if len(a.seed)>3:raise SystemExit('maximum three albums per lab acquisition')
    asyncio.run(collect(a))
