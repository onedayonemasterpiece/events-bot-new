#!/usr/bin/env python3
"""Vector-first recall for a frozen future-event evidence export.

This tool never mutates Fly SQLite or the Supabase sidecar. Its pairs are recall
candidates only and require source-grounded LLM/human adjudication.
"""
from __future__ import annotations
import argparse
import asyncio, gzip, json, math, os, re, sys, urllib.parse, urllib.request
from pathlib import Path
from collections import defaultdict
ROOT=Path(__file__).resolve().parents[2]; sys.path.insert(0,str(ROOT))
EXPORT: Path
OUT: Path
CACHE: Path

def load_env(path):
 for line in Path(path).read_text(errors='ignore').splitlines():
  s=line.strip()
  if not s or s.startswith('#') or '=' not in s:continue
  k,v=s.split('=',1);os.environ.setdefault(k.strip(),v.strip().strip('"').strip("'"))
def clean(v):return re.sub(r'\s+',' ',str(v or '')).strip()
def load_rows():
 o=json.loads(gzip.open(EXPORT,'rt',encoding='utf-8').read()); rows=o['events'];sources=defaultdict(list);posters=defaultdict(list)
 for s in o.get('event_sources', o.get('event_source', [])):sources[int(s['event_id'])].append(s)
 for p in o.get('event_posters', o.get('eventposter', [])):posters[int(p['event_id'])].append(p)
 return o,rows,sources,posters
def make_doc(ev,sources,posters):
 src=[]
 for s in sources:
  if clean(s.get('source_text')):src.append(f"[{s.get('source_type')} {s.get('source_url')}] {clean(s.get('source_text'))[:1000]}")
 if clean(ev.get('source_text')):src.append(clean(ev['source_text'])[:1000])
 try:
  for x in json.loads(ev.get('source_texts') or '[]'):
   if clean(x):src.append(clean(x)[:800])
 except Exception:pass
 pdocs=[{'ocr_title':p.get('ocr_title'),'ocr_text':p.get('ocr_text'),'phash':p.get('phash') or p.get('poster_hash')} for p in posters]
 from event_identity import build_identity_candidate_document
 return build_identity_candidate_document({'title':ev.get('title'),'date':ev.get('date'),'time':ev.get('time'),'end_date':ev.get('end_date'),'location_name':ev.get('location_name'),'location_address':ev.get('location_address'),'city':ev.get('city'),'event_type':ev.get('event_type'),'festival':ev.get('festival'),'ticket_status':ev.get('ticket_status'),'ticket_link':ev.get('ticket_link'),'source_type':'future_quality_audit','source_url':ev.get('source_post_url') or ev.get('source_vk_post_url'),'search_digest':ev.get('search_digest'),'raw_excerpt':ev.get('short_description') or ev.get('description'),'source_text':'\n'.join(dict.fromkeys(src))[:2800],'posters':pdocs},max_chars=2600,source_text_max_chars=1200)
def parse_vector(v):
 if isinstance(v,list):return [float(x) for x in v]
 s=str(v or '').strip()
 if s.startswith('['):return [float(x) for x in json.loads(s)]
 return []
def fetch_supabase_vectors(ids):
 base=os.environ['PERSONALIZATION_SUPABASE_URL'].rstrip('/');key=os.environ['PERSONALIZATION_SUPABASE_SECRET_KEY'];h={'apikey':key,'Authorization':'Bearer '+key,'Accept':'application/json'};out={}
 for start in range(0,len(ids),70):
  chunk=ids[start:start+70];ins=','.join(map(str,chunk));path=f"event_embeddings?select=event_id,embedding,embedding_model,embedding_dim,embedding_doc_kind,text_hash&embedding_doc_kind=eq.related_v1&embedding_model=eq.gemini-embedding-2&embedding_dim=eq.768&event_id=in.({ins})"
  with urllib.request.urlopen(urllib.request.Request(base+'/rest/v1/'+path,headers=h),timeout=30) as r:rows=json.load(r)
  for row in rows:
   vec=parse_vector(row.get('embedding'))
   if len(vec)==768:out[int(row['event_id'])]=vec
 return out
def cos(a,b):
 dot=sum(x*y for x,y in zip(a,b));aa=sum(x*x for x in a);bb=sum(x*x for x in b);return dot/math.sqrt(aa*bb) if aa and bb else 0
async def main(args):
 global EXPORT, OUT, CACHE
 EXPORT=args.export.resolve(); OUT=args.output.resolve(); OUT.mkdir(parents=True,exist_ok=True); CACHE=(args.cache or (OUT/'embeddings.jsonl')).resolve()
 if args.env_file: load_env(args.env_file)
 os.environ.update({'GOOGLE_AI_ALLOW_RESERVE_FALLBACK':'0','GOOGLE_AI_LOCAL_LIMITER_FALLBACK':'0','GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR':'0','GOOGLE_AI_FALLBACK_MODELS':'','GOOGLE_AI_PROVIDER_TIMEOUT_SEC':'45','GOOGLE_AI_RESERVE_RPC_RECHECK_SECONDS':'30'})
 meta,rows,sources,posters=load_rows();docs={int(e['id']):make_doc(e,sources[int(e['id'])],posters[int(e['id'])]) for e in rows};ids=sorted(docs)
 vec=fetch_supabase_vectors(ids)
 local={}
 if CACHE.exists():
  for ln in CACHE.read_text().splitlines():
   try:o=json.loads(ln);local[(int(o['event_id']),o['sha256'])]=o['embedding']
   except Exception:pass
 for eid,d in docs.items():
  if (eid,d.sha256) in local:vec[eid]=local[(eid,d.sha256)]
 missing=[eid for eid in ids if eid not in vec]
 print(json.dumps({'stage':'start','events':len(ids),'supabase_or_cache':len(vec),'missing':len(missing),'captured_at':meta.get('captured_at_utc') or meta.get('cutoff_utc')},ensure_ascii=False),flush=True)
 if missing:
  from supabase import create_client
  from google_ai import GoogleAIClient,SecretsProvider
  sb=create_client(os.environ['SUPABASE_URL'],os.environ['SUPABASE_KEY']);client=GoogleAIClient(supabase_client=sb,secrets_provider=SecretsProvider(),consumer='future_event_quality_audit',account_name='future-event-quality-audit',default_env_var_name='GOOGLE_API_KEY4');client.provider_timeout_seconds=45
  for idx,eid in enumerate(missing,1):
   d=docs[eid]
   while True:
    try:
     vals,_=await client.embed_content_async(model='gemini-embedding-2',text=d.text,output_dimensionality=768)
     break
    except Exception as exc:
     if exc.__class__.__name__ == 'RateLimitError' and getattr(exc,'blocked_reason',None) in {'rpm','tpm'}:
      wait=max(5.0,float(getattr(exc,'retry_after_ms',None) or 60000)/1000+1.0)
      print(json.dumps({'stage':'rate_wait','seconds':wait,'reason':getattr(exc,'blocked_reason',None)}),flush=True)
      await asyncio.sleep(wait)
      continue
     raise
   emb=[float(x) for x in vals];vec[eid]=emb
   with CACHE.open('a') as f:f.write(json.dumps({'event_id':eid,'sha256':d.sha256,'embedding':emb},ensure_ascii=False,separators=(',',':'))+'\n')
   print(json.dumps({'stage':'embedding','done':idx,'total':len(missing),'event_id':eid}),flush=True)
 pairs=[]
 for i,a in enumerate(ids):
  sims=sorted(((cos(vec[a],vec[b]),b) for b in ids if b!=a),reverse=True)[:10]
  for score,b in sims:
   if a<b:pairs.append((a,b,score))
 best={}
 for a,b,s in pairs:best[(a,b)]=max(best.get((a,b),0),s)
 by={int(e['id']):e for e in rows};en=[]
 for (a,b),s in sorted(best.items(),key=lambda x:(-x[1],x[0])):
  fields=['title','date','time','end_date','location_name','location_address','city','event_type','source_post_url','source_vk_post_url','tg_event_post_url','telegraph_url']
  en.append({'left_id':a,'right_id':b,'similarity':round(s,6),'left':{k:by[a].get(k) for k in fields},'right':{k:by[b].get(k) for k in fields},'left_source_count':len(sources[a]),'right_source_count':len(sources[b])})
 (OUT/'vector_pairs.json').write_text(json.dumps(en,ensure_ascii=False,indent=2))
 print(json.dumps({'stage':'done','events':len(ids),'vectors':len(vec),'pairs':len(en),'gte_09':sum(x['similarity']>=.9 for x in en),'gte_085':sum(x['similarity']>=.85 for x in en),'gte_08':sum(x['similarity']>=.8 for x in en)},ensure_ascii=False),flush=True)
def parse_args():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--export',type=Path,required=True,help='Frozen JSON or JSON.GZ export with events and linked evidence')
 p.add_argument('--output',type=Path,required=True,help='Ignored artifact output directory')
 p.add_argument('--cache',type=Path,help='Optional ignored embedding JSONL cache')
 p.add_argument('--env-file',type=Path,help='Optional local env file; never written to output')
 return p.parse_args()
if __name__=='__main__':asyncio.run(main(parse_args()))
