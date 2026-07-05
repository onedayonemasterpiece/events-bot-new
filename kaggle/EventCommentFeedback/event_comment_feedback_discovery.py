from __future__ import annotations
import asyncio, base64, csv, datetime as dt, gzip, hashlib, html, json, math, os, random, re, subprocess, sys, tarfile, time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

RUN_SCHEMA_VERSION = "event-comment-feedback-kaggle-dual-local-v1"
PHRASE_BANK_VERSION = "event-comment-feedback-phrase-bank-v1"
REQUIRED_MODELS = ["intfloat/multilingual-e5-base", "BAAI/bge-m3"]
GATE_MODEL = "intfloat/multilingual-e5-base"
SCRIPT_DIR = Path(globals().get('__file__', Path.cwd() / 'event_comment_feedback_discovery.py')).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

WORK = Path('/kaggle/working') if Path('/kaggle/working').exists() else Path.cwd()
STATUS_PATH = WORK / 'event_comment_feedback_status.jsonl'
STATUS_CLIENT = None
STATUS_RESOURCES: list[str] = []
STATUS_PROGRESS: dict[str, Any] = {
    'phase': 'bootstrap',
    'progress_percent': 0,
    'progress_label': 'подготовка',
}

EVENT_PHASES = {
    'kernel_started': 'preflight',
    'pip_install_start': 'preflight',
    'pip_install_done': 'preflight',
    'secrets_loaded': 'preflight',
    'preflight_ok': 'preflight',
    'fetch_tg_progress': 'fetch',
    'fetch_vk_progress': 'fetch',
    'fetch_done': 'fetch',
    'model_load_start': 'embed',
    'model_encode_start': 'embed',
    'model_encode_done': 'embed',
    'report_written': 'report',
    'kernel_done': 'report',
    'kernel_failed': 'failed',
}

def _find_status_client_loader():
    try:
        from kaggle_status_client import load_status_client  # type: ignore
        return load_status_client
    except Exception:
        pass
    for root in [SCRIPT_DIR, Path.cwd(), WORK, Path('/kaggle/input')]:
        if not root.exists():
            continue
        try:
            matches = sorted(root.rglob('kaggle_status_client.py'))
        except Exception:
            matches = []
        for path in matches:
            try:
                import importlib.util
                spec = importlib.util.spec_from_file_location('kaggle_status_client_dynamic', path)
                if not spec or not spec.loader:
                    continue
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return getattr(mod, 'load_status_client', None)
            except Exception:
                continue
    return None

def init_status() -> None:
    global STATUS_CLIENT
    loader = _find_status_client_loader()
    if loader is None:
        return
    try:
        STATUS_CLIENT = loader(output_dir=WORK, log=lambda message: print(message, flush=True))
    except Exception as exc:
        print(f'[event-comment-feedback] status init failed: {type(exc).__name__}: {exc}', flush=True)
        STATUS_CLIENT = None

def _status_enabled() -> bool:
    return bool(STATUS_CLIENT is not None and getattr(STATUS_CLIENT, 'enabled', False))

def _status_event(event: str, *, payload: dict[str, Any], status: str | None = None, message: str | None = None) -> None:
    if not _status_enabled():
        return
    phase = str(payload.get('phase') or EVENT_PHASES.get(event) or event)
    for key in ('progress_percent', 'progress_label', 'done', 'total', 'events', 'source_posts', 'selected', 'tg', 'vk', 'comments', 'errors', 'model', 'count'):
        if key in payload:
            STATUS_PROGRESS[key] = payload[key]
    STATUS_PROGRESS['phase'] = phase
    try:
        STATUS_CLIENT.event(
            event,
            phase=phase,
            status=status or ('done' if event in {'report_written', 'kernel_done'} else 'failed' if event == 'kernel_failed' else 'running'),
            progress=dict(STATUS_PROGRESS),
            message=message,
        )
    except Exception as exc:
        print(f'[event-comment-feedback] status event failed {event}: {type(exc).__name__}: {exc}', flush=True)

def acquire_status_resources() -> None:
    if not _status_enabled():
        return
    for key in STATUS_CLIENT.config.get('resource_leases') or []:
        if not STATUS_CLIENT.acquire_resource(str(key), ttl_seconds=3 * 60 * 60):
            raise RuntimeError(f'Required Kaggle resource is busy: {key}')
        STATUS_RESOURCES.append(str(key))
    STATUS_CLIENT.start_alive(interval_seconds=60, progress_provider=lambda: dict(STATUS_PROGRESS))

def finish_status() -> None:
    if STATUS_CLIENT is None:
        return
    for key in list(STATUS_RESOURCES):
        try:
            STATUS_CLIENT.release_resource(key)
            STATUS_RESOURCES.remove(key)
        except Exception as exc:
            print(f'[event-comment-feedback] status resource release failed {key}: {exc}', flush=True)
    try:
        STATUS_CLIENT.stop_alive()
    except Exception:
        pass

def emit(event: str, **payload: Any) -> None:
    row = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "event": event, **payload}
    print(json.dumps(row, ensure_ascii=False), flush=True)
    with STATUS_PATH.open('a', encoding='utf-8') as f:
        f.write(json.dumps(row, ensure_ascii=False)+'\n')
    _status_event(event, payload=payload, message=payload.get('message') if isinstance(payload.get('message'), str) else None)


def human_delay(base_seconds: float, *, index: int = 0, platform: str = '') -> float:
    base = max(0.2, float(base_seconds or 0.45))
    # Human-like: non-uniform pauses, occasional longer breath between batches.
    delay = random.uniform(base * 0.75, base * 1.85)
    if index and index % 17 == 0:
        delay += random.uniform(2.0, 5.0)
    if index and index % 53 == 0:
        delay += random.uniform(8.0, 18.0)
    return min(delay, 35.0)

async def async_human_pause(base_seconds: float, *, index: int = 0, platform: str = '') -> None:
    await asyncio.sleep(human_delay(base_seconds, index=index, platform=platform))

def sync_human_pause(base_seconds: float, *, index: int = 0, platform: str = '') -> None:
    time.sleep(human_delay(base_seconds, index=index, platform=platform))

def ensure_deps() -> None:
    missing=[]
    for mod,pkg in [('telethon','telethon'),('openpyxl','openpyxl'),('sentence_transformers','sentence-transformers'),('cryptography','cryptography'),('requests','requests')]:
        try: __import__(mod)
        except Exception: missing.append(pkg)
    if missing:
        emit('pip_install_start', packages=missing)
        subprocess.check_call([sys.executable,'-m','pip','install','-q',*missing])
        emit('pip_install_done', packages=missing)

PAYLOAD_EXTRACTED = False

def extract_payload_tarball_once() -> None:
    global PAYLOAD_EXTRACTED
    if PAYLOAD_EXTRACTED:
        return
    PAYLOAD_EXTRACTED = True
    for root in [Path('/kaggle/input'), Path.cwd(), WORK]:
        if not root.exists():
            continue
        for archive in root.rglob('event_comment_feedback_payload.tarball'):
            target = WORK / 'event_comment_feedback_payload'
            target.mkdir(parents=True, exist_ok=True)
            with tarfile.open(archive, 'r:*') as tf:
                tf.extractall(target)
            emit('payload_extracted', archive=str(archive), target=str(target), progress_percent=3, progress_label='payload распакован')
            return

def find_input_file(name: str) -> Path:
    for attempt in range(2):
        for root in [Path('/kaggle/input'), Path.cwd(), WORK]:
            if not root.exists(): continue
            for p in root.rglob(name): return p
        if attempt == 0:
            extract_payload_tarball_once()
    raise FileNotFoundError(name)

def load_json_any(path: Path) -> Any:
    data = path.read_bytes()
    if path.suffix == '.gz': data = gzip.decompress(data)
    return json.loads(data.decode('utf-8'))

def load_secrets() -> dict[str, Any]:
    from cryptography.fernet import Fernet
    enc = find_input_file('secrets.enc').read_bytes(); key = find_input_file('fernet.key').read_bytes().strip()
    secrets = json.loads(Fernet(key).decrypt(enc).decode('utf-8'))
    for k,v in secrets.items():
        if v is not None and str(v).strip(): os.environ.setdefault(k, str(v))
    emit('secrets_loaded', keys=sorted(secrets.keys()))
    return secrets

def decode_bundle(name: str):
    raw=(os.getenv(name) or '').strip()
    if not raw: raise RuntimeError(f'{name} missing')
    b=json.loads(base64.urlsafe_b64decode(raw.encode('ascii')).decode('utf-8'))
    kwargs={}
    for key in ('device_model','system_version','app_version','lang_code','system_lang_code'):
        if b.get(key): kwargs[key]=str(b[key])
    return b.get('session') or '', kwargs

def normalize_text(text: str) -> str:
    text=html.unescape(str(text or ''))
    text=re.sub(r'https?://\S+',' ',text); text=re.sub(r'[\uFE0F\u200D]','',text)
    return re.sub(r'\s+',' ',text).strip()
def low(text: str) -> str: return normalize_text(text).lower().replace('ё','е')
def short_hash(value: str, n:int=16) -> str: return hashlib.sha256(value.encode('utf-8', errors='ignore')).hexdigest()[:n]
def is_link_or_emoji_only(text: str) -> bool: return len(re.findall(r'[A-Za-zА-Яа-яЁё]', re.sub(r'https?://\S+','',text or ''))) < 2
def is_probable_source_copy(text: str) -> bool:
    t=low(text)
    if len(t)>760: return True
    formal=sum(1 for w in ['приглашаем','состоится','выставка','концерт','программа','регистрация','билеты','подробнее','начало','стоимость','адрес','организатор'] if w in t)
    user=any(w in t for w in ['жду','ждём','ждем','пойду','идём','идем','хочу','ура','класс','спасибо','?','жаль','дорого','подскажите','можно','интересно','люблю'])
    return len(t)>320 and formal>=3 and not user

def build_fetch_posts(manifest: dict[str,Any]):
    events=manifest['events']; grouped={}
    for link in sorted(manifest['source_links'], key=lambda s:(-int(s.get('metric_comments') or 0), events.get(str(s['event_id']),{}).get('date') or '', int(s['event_id']), s['platform_post_key'])):
        parsed=link.get('parsed') or {}; platform=link.get('platform')
        if platform not in {'telegram','vk'}: continue
        key=link['platform_post_key']; item=grouped.setdefault(key, {'platform':platform,'platform_post_key':key,'parsed':parsed,'links':[],'metric_comments':0,'source_urls':[]})
        item['links'].append({'event_id':link['event_id'],'event_source_id':link.get('event_source_id'),'source_url':link.get('source_url')})
        item['metric_comments']=max(int(item['metric_comments']), int(link.get('metric_comments') or 0))
        if link.get('source_url') and link.get('source_url') not in item['source_urls']: item['source_urls'].append(link.get('source_url'))
    selected=[]; skipped=[]
    for p in sorted(grouped.values(), key=lambda p:(-int(p.get('metric_comments') or 0), p['platform_post_key'])):
        parsed=p.get('parsed') or {}
        if p['platform']=='telegram' and not parsed.get('username'): skipped.append({**p,'skip_reason':'telegram_private_or_chat_id_without_username'})
        elif p['platform']=='vk' and (parsed.get('owner_id') is None or parsed.get('post_id') is None): skipped.append({**p,'skip_reason':'vk_missing_owner_or_post_id'})
        else: selected.append(p)
    return selected, skipped

async def fetch_tg(posts, max_comments:int, sleep_s:float):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import FloodWaitError
    api_id=int(os.getenv('TG_API_ID') or os.getenv('TELEGRAM_API_ID') or '0'); api_hash=os.getenv('TG_API_HASH') or os.getenv('TELEGRAM_API_HASH') or ''
    session, kwargs=decode_bundle('TELEGRAM_AUTH_BUNDLE_DISCOVERY')
    client=TelegramClient(StringSession(session), api_id, api_hash, **kwargs)
    comments=[]; errors=[]; account={}
    await client.connect()
    try:
        if not await client.is_user_authorized(): raise RuntimeError('TELEGRAM_AUTH_BUNDLE_DISCOVERY unauthorized')
        me=await client.get_me(); account={'id':int(me.id),'username':getattr(me,'username',None),'bundle':'TELEGRAM_AUTH_BUNDLE_DISCOVERY'}
        for idx,p in enumerate(posts,1):
            parsed=p['parsed']; username=parsed.get('username'); mid=int(parsed.get('message_id') or 0)
            try:
                entity=await client.get_entity(username); got=0
                async for msg in client.iter_messages(entity, reply_to=mid, limit=max_comments):
                    txt=normalize_text(getattr(msg,'message',None) or '')
                    if not txt or getattr(msg,'post',False) or getattr(msg,'fwd_from',None) or is_link_or_emoji_only(txt) or is_probable_source_copy(txt): continue
                    sender_id=getattr(msg,'sender_id',None)
                    comments.append({'platform':'telegram','platform_post_key':p['platform_post_key'],'source_urls':p['source_urls'],'comment_key':f'tg:{username}:{mid}:{msg.id}','created_at':getattr(msg,'date',None).isoformat() if getattr(msg,'date',None) else None,'author_hash':short_hash(f'tg:{sender_id}' if sender_id else f'tgmsg:{msg.id}'),'text':txt,'links':p['links']}); got+=1
                if idx%25==0: emit('fetch_tg_progress', done=idx,total=len(posts),comments=len(comments),errors=len(errors), progress_label=f'TG источники {idx}/{len(posts)} · комментарии {len(comments)}')
                await async_human_pause(sleep_s, index=idx, platform='telegram')
            except FloodWaitError as exc:
                errors.append({'platform':'telegram','platform_post_key':p['platform_post_key'],'status':'flood_wait','seconds':int(exc.seconds)})
                if exc.seconds<=45: await asyncio.sleep(exc.seconds)
                else: break
            except Exception as exc:
                errors.append({'platform':'telegram','platform_post_key':p['platform_post_key'],'status':'error','error_type':type(exc).__name__,'message':str(exc)[:240]})
    finally: await client.disconnect()
    return comments, errors, account

def fetch_vk(posts, max_comments:int, sleep_s:float):
    import requests
    token=(os.getenv('VK_SERVICE_TOKEN') or os.getenv('VK_SERVICE_KEY') or os.getenv('VK_ACCESS_TOKEN') or '').strip(); comments=[]; errors=[]
    if not token: return comments, [{'platform':'vk','status':'missing_token'}]
    for idx,p in enumerate(posts,1):
        parsed=p['parsed']; owner=parsed.get('owner_id'); post=parsed.get('post_id')
        try:
            params={'owner_id':int(owner),'post_id':int(post),'count':max_comments,'need_likes':0,'thread_items_count':0,'v':'5.199','access_token':token,'sort':'asc'}
            data=requests.get('https://api.vk.com/method/wall.getComments', params=params, timeout=25).json()
            if 'error' in data:
                errors.append({'platform':'vk','platform_post_key':p['platform_post_key'],'status':'api_error','code':data['error'].get('error_code'),'message':data['error'].get('error_msg')}); continue
            for item in ((data.get('response') or {}).get('items') or []):
                txt=normalize_text(item.get('text') or '')
                if not txt or is_link_or_emoji_only(txt) or is_probable_source_copy(txt): continue
                comments.append({'platform':'vk','platform_post_key':p['platform_post_key'],'source_urls':p['source_urls'],'comment_key':f"vk:{owner}:{post}:{item.get('id')}",'created_at':dt.datetime.fromtimestamp(int(item.get('date') or 0), dt.timezone.utc).isoformat() if item.get('date') else None,'author_hash':short_hash(f"vk:{item.get('from_id')}"),'text':txt,'links':p['links']})
            if idx%50==0: emit('fetch_vk_progress', done=idx,total=len(posts),comments=len(comments),errors=len(errors), progress_label=f'VK источники {idx}/{len(posts)} · комментарии {len(comments)}')
            sync_human_pause(sleep_s, index=idx, platform='vk')
        except Exception as exc: errors.append({'platform':'vk','platform_post_key':p['platform_post_key'],'status':'error','error_type':type(exc).__name__,'message':str(exc)[:240]})
    return comments, errors

def quoted_values(line: str): return [normalize_text(v) for v in re.findall(r'[“\"]([^”\"]+)[”\"]', line) if normalize_text(v)]
def parse_phrase_bank(path: Path):
    text=path.read_text(encoding='utf-8'); parts=re.split(r'(?m)^###\s+(\d+)\.\s+`([^`]+)`\s*$', text); out=[]
    for idx in range(1,len(parts),3):
        p={'num':int(parts[idx]),'id':parts[idx+1],'category':'','signal_type':'','tone':'internal','icon':'','risk':'internal','vector_only_allowed':False,'requires_llm_verification':True,'publishable':True,'public_sentence':None,'min_evidence_count':2,'min_unique_authors':2,'positive_prototypes':[],'hard_negatives':[]}
        for raw in parts[idx+2].splitlines():
            line=raw.strip()
            if line.startswith('- **Category:**'): p['category']=normalize_text(line.split(':**',1)[1])
            elif line.startswith('- **signal_type:**'):
                m=re.search(r'`([^`]+)`',line); p['signal_type']=m.group(1) if m else ''
            elif line.startswith('- **tone/icon/risk:**'):
                vals=re.findall(r'`([^`]+)`',line);
                if len(vals)>=3: p['tone'],p['icon'],p['risk']=vals[:3]
            elif line.startswith('- **Policy:**'): p['vector_only_allowed']='vector_only_allowed=true' in line; p['requires_llm_verification']='requires_llm_verification=true' in line
            elif line.startswith('- **public_sentence:**'):
                if '`null`' in line or re.search(r'\bnull\b',line,re.I): p['public_sentence']=None; p['publishable']=False
                else:
                    vals=quoted_values(line); p['public_sentence']=vals[0] if vals else None
            elif line.startswith('- **min evidence:**'):
                m1=re.search(r'min_evidence_count=(\d+)',line); m2=re.search(r'min_unique_authors=(\d+)',line)
                if m1: p['min_evidence_count']=int(m1.group(1))
                if m2: p['min_unique_authors']=int(m2.group(1))
            elif line.startswith('- **Positive prototypes:**'): p['positive_prototypes']=quoted_values(line)
            elif line.startswith('- **Hard negatives:**'): p['hard_negatives']=quoted_values(line)
        if not p['public_sentence']: p['publishable']=False
        out.append(p)
    return out

def phrase_doc(p): return ' | '.join([str(p.get('public_sentence') or p['id']), str(p.get('signal_type') or ''), '; '.join(p.get('positive_prototypes') or [])])
def neg_doc(p): return ' | '.join(p.get('hard_negatives') or [])
def prefix(model,text,is_query): return (('query: ' if is_query else 'passage: ') + text) if 'multilingual-e5' in model.lower() else text

def sparse_counter(text):
    c=Counter();
    for w in re.findall(r'[a-zа-я0-9]+', low(text)):
        if len(w)>2:
            c[f'w:{w}']+=2; pad=f' {w} '
            for n in (3,4):
                for i in range(max(0,len(pad)-n+1)): c[f'c:{pad[i:i+n]}']+=1
        else: c[f'w:{w}']+=1
    return c
def cos_counter(a,b):
    if not a or not b: return 0.0
    if len(a)>len(b): a,b=b,a
    dot=sum(v*b.get(k,0) for k,v in a.items());
    if dot<=0: return 0.0
    return float(dot/(math.sqrt(sum(v*v for v in a.values()))*math.sqrt(sum(v*v for v in b.values()))))
QUESTION_IDS={'ticket_availability_question','time_questions','duration_questions','children_questions','location_questions','pushkin_card_questions','accessibility_questions','parking_questions','payment_questions','age_limit_questions','online_recording_questions','registration_interest','extra_places_question','extra_date_request','refund_exchange_questions'}
RESALE_RE=re.compile(r'(?i)(\b(?:продам|продаю|куплю|ищу|нужен|нужны)\b[^\n]{0,40}\bбилет|\bбилет[^\n]{0,40}\b(?:продам|продаю|куплю|ищу|нужен|нужны)|есть\s+у\s+кого|у\s+кого(?:-то)?\s+есть|лишн\w*\s+билет|напишите\s+(?:пожалуйста\s+)?(?:в\s+)?(?:личк|лс))')
QUESTION_RE=re.compile(r'(?i)(\?|подскаж\w*|можно\s+ли|есть\s+ли|будет\s+ли|остал\w*|где\b|когда\b|во\s+сколько|сколько\b|как\b|нужн\w*\s+ли|регистрац\w*)')
GUARDS={'children_questions':[r'дет',r'реб[её]н',r'школь',r'возраст'],'accessibility_questions':[r'пандус',r'маломобиль',r'инвалид',r'коляск',r'лифт'],'weather_concern':[r'дожд',r'погод',r'ливн',r'гр[оа]з',r'ветер'],'ticket_availability_question':[r'билет',r'мест',r'регистрац',r'попасть'],'ticket_interest_high':[r'билет',r'мест',r'регистрац',r'попасть'],'high_demand_from_ticket_friction':[r'билет',r'мест',r'регистрац',r'попасть',r'тираж',r'успева'],'time_questions':[r'во сколько',r'время',r'начал',r'вход',r'длит']}
def guard(text,pid):
    t=low(text); reasons=[]
    if pid in QUESTION_IDS:
        if RESALE_RE.search(t): reasons.append('ticket_resale_or_private_ticket_request')
        elif not QUESTION_RE.search(t): reasons.append('question_phrase_without_question_marker')
    pats=GUARDS.get(pid)
    if pats and not any(re.search(p,t,re.I) for p in pats): reasons.append('phrase_lexical_guard')
    return reasons

def encode_model(model_name, texts, is_query):
    from sentence_transformers import SentenceTransformer
    emit('model_load_start', model=model_name, progress_percent=45, progress_label=f'загрузка модели {model_name}')
    model=SentenceTransformer(model_name, device=os.getenv('ACQ_COMMENT_RETRIEVAL_DEVICE') or None)
    try: model.max_seq_length=128
    except Exception: pass
    batch=32 if 'multilingual-e5' in model_name.lower() else 8
    emit('model_encode_start', model=model_name, count=len(texts), is_query=is_query, batch=batch, progress_label=f'embeddings {model_name}: {len(texts)} текстов')
    arr=model.encode([prefix(model_name,t,is_query) for t in texts], batch_size=batch, normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=True)
    emit('model_encode_done', model=model_name, count=len(texts), is_query=is_query, progress_label=f'embeddings готовы {model_name}')
    return arr

def score(comments, manifest, phrases):
    publish=[p for p in phrases if p.get('publishable') and p.get('public_sentence')]
    pdocs=[phrase_doc(p) for p in publish]; ndocs=[neg_doc(p) for p in publish]
    unique={}
    event_comments=defaultdict(list)
    for c in comments:
        for link in c.get('links') or []:
            item=dict(c); item['event_id']=int(link['event_id']); event_comments[item['event_id']].append(item)
            th=short_hash(low(c['text']),24); unique.setdefault(th, {'text_hash':th,'text':c['text']})
    ulist=list(unique.values()); texts=[u['text'] for u in ulist]
    model_scores={}
    for model in REQUIRED_MODELS:
        pos=encode_model(model,pdocs,True); neg=encode_model(model,ndocs,True); cvec=encode_model(model,texts,False)
        rows={}
        for ci,u in enumerate(ulist):
            vals=sorted([(float(cvec[ci] @ pos[pi]), p['id'], pi) for pi,p in enumerate(publish)], reverse=True)
            top, pid, pi=vals[0]; second, spid, _=vals[1] if len(vals)>1 else (0.0,None,-1); ns=float(cvec[ci] @ neg[pi]) if ndocs[pi] else 0.0
            rows[u['text_hash']]={'top_phrase_id':pid,'top_score':top,'second_phrase_id':spid,'second_score':second,'margin_neg':top-ns,'top5':{pid2:rank for rank,(_s,pid2,_pi) in enumerate(vals[:5],1)},'scores5':{pid2:s for s,pid2,_pi in vals[:5]}}
        model_scores[model]=rows
    by_phrase={p['id']:p for p in publish}; sparse={p['id']:sparse_counter(phrase_doc(p)) for p in publish}
    event_results={}; evidence=[]
    for eid, rows in event_comments.items():
        seen=set(); groups=defaultdict(lambda:{'phrase':None,'comments':[],'authors':set(),'sources':set(),'scores':[]})
        for c in rows:
            th=short_hash(low(c['text']),24); ah=c.get('author_hash') or ''; k=(th,ah)
            if k in seen: continue
            seen.add(k)
            e5=model_scores[REQUIRED_MODELS[0]].get(th); bge=model_scores[REQUIRED_MODELS[1]].get(th)
            if not e5 or not bge: continue
            pid=e5['top_phrase_id']; p=by_phrase.get(pid); bge_rank=bge.get('top5',{}).get(pid,999); reasons=guard(c['text'],pid)
            ss=cos_counter(sparse_counter(c['text']), sparse.get(pid, Counter()))
            if ss < 0.01: reasons.append('sparse_support_low')
            if e5['margin_neg'] < -0.015: reasons.append('negative_margin_low')
            if bge_rank > 3: reasons.append('bge_not_top3')
            if reasons: continue
            cand={'e5_score':round(e5['top_score'],4),'bge_top_phrase_id':bge['top_phrase_id'],'bge_rank_for_e5_phrase':bge_rank,'bge_same_phrase_score':round(float(bge.get('scores5',{}).get(pid,0.0)),4),'sparse_score':round(ss,4),'model_agreement':bge['top_phrase_id']==pid}
            g=groups[pid]; g['phrase']=p; item=dict(c); item['candidate']=cand; g['comments'].append(item); g['authors'].add(ah); g['sources'].add(c.get('platform_post_key')); g['scores'].append(cand)
        accepted=[]; other=[]
        for pid,g in groups.items():
            p=g['phrase']; ev=len(g['comments']); au=len(g['authors']); min_ev=int(p.get('min_evidence_count') or 2); min_au=int(p.get('min_unique_authors') or 2)
            if ev>=min_ev and au>=min_au and p.get('vector_only_allowed') and not p.get('requires_llm_verification') and p.get('risk')=='low': status='public_ready_dual_kaggle'
            elif ev>=min_ev and au>=min_au: status='needs_review_dual_kaggle'
            else: status='suppressed_weak_dual_kaggle'
            cs=sorted(g['comments'], key=lambda x:x['candidate']['e5_score'], reverse=True)
            rec={'phrase_id':pid,'tone':p.get('tone'),'risk_class':p.get('risk'),'public_sentence':p.get('public_sentence'),'evidence_count':ev,'unique_authors_count':au,'sources_count':len(g['sources']),'status':status,'evidence_snippets':[c['text'][:220] for c in cs[:4]],'score_samples':[{**c['candidate'],'comment_key':c['comment_key'],'text_snippet':c['text'][:220]} for c in cs[:4]]}
            for c in cs[:4]: evidence.append({'event_id':eid,'phrase_id':pid,'status':status,**c['candidate'],'comment_key':c['comment_key'],'text_snippet':c['text'][:220]})
            (accepted if status.startswith('public_ready') else other).append(rec)
        event_results[str(eid)]={'event':manifest['events'].get(str(eid),{'id':eid}),'comments_seen_count':len(rows),'dedup_comments_count':len(seen),'accepted_items':accepted,'review_or_suppressed_items':other}
    return event_results,evidence

def write_reports(manifest, comments, errors, event_results, evidence):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    summary={'schema_version':RUN_SCHEMA_VERSION,'generated_at':dt.datetime.now(dt.timezone.utc).isoformat(),'embedding_api_allowed':False,'provider_calls_total_this_process':0,'read_mode':'human_like_api_paced_v1','models':REQUIRED_MODELS,'events_in_manifest':manifest.get('event_count'),'source_links':manifest.get('source_link_count'),'source_posts':manifest.get('source_post_count'),'comments_fetched':len(comments),'events_with_comments':len(event_results),'fetch_errors':len(errors),'events_with_public_ready':sum(1 for r in event_results.values() if r['accepted_items']),'events_with_review_candidates':sum(1 for r in event_results.values() if any(i['status']=='needs_review_dual_kaggle' for i in r['review_or_suppressed_items']))}
    (WORK/'event_comment_feedback_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    (WORK/'event_comment_feedback_probe.json').write_text(json.dumps({**summary,'events':event_results}, ensure_ascii=False, indent=2), encoding='utf-8')
    rows=[]
    for eid,res in sorted(event_results.items(), key=lambda kv:((kv[1]['event'].get('date') or ''), int(kv[0]))):
        ev=res['event']; items=res['accepted_items']+res['review_or_suppressed_items']
        def join(status_prefix,tone=None):
            vals=[]
            for it in items:
                if not it['status'].startswith(status_prefix): continue
                if tone and it.get('tone')!=tone: continue
                vals.append(f"{it['public_sentence']} ({it['evidence_count']}/{it['unique_authors_count']})")
            return '\n'.join(vals[:6])
        rows.append({'event_id':int(eid),'date':ev.get('date'),'time':ev.get('time'),'title':ev.get('title'),'venue':ev.get('location_name'),'comments_seen':res['comments_seen_count'],'dedup_comments':res['dedup_comments_count'],'public_positive':join('public_ready','positive'),'public_neutral':join('public_ready','neutral'),'public_negative':join('public_ready','concern'),'review_positive':join('needs_review','positive'),'review_neutral':join('needs_review','neutral'),'review_negative':join('needs_review','concern'),'suppressed_weak_top':join('suppressed_weak'),'evidence_snippets':'\n---\n'.join([sn for it in items[:6] for sn in it.get('evidence_snippets',[])][:6]),'note':'Kaggle dual local embeddings: intfloat/multilingual-e5-base + BAAI/bge-m3; embedding API disabled'})
    headers=list(rows[0].keys()) if rows else ['note']
    with (WORK/'event_comment_feedback_full_table.csv').open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=headers); w.writeheader(); w.writerows(rows or [{'note':'No rows'}])
    wb=Workbook(); ws=wb.active; ws.title='summary'
    for k,v in summary.items(): ws.append([k,json.dumps(v,ensure_ascii=False) if isinstance(v,(list,dict,bool)) else v])
    ws.column_dimensions['A'].width=38; ws.column_dimensions['B'].width=90
    evs=wb.create_sheet('events'); evs.append(headers)
    for cell in evs[1]: cell.font=Font(bold=True); cell.fill=PatternFill('solid', fgColor='D9EAD3')
    for r in rows: evs.append([r.get(h,'') for h in headers])
    for sh in wb.worksheets:
        for row in sh.iter_rows():
            for cell in row: cell.alignment=Alignment(wrap_text=True, vertical='top')
    evs.freeze_panes='A2'
    for col,w in {'A':10,'B':12,'D':46,'E':30,'H':52,'I':52,'J':52,'K':52,'L':52,'M':52,'N':52,'O':70,'P':50}.items(): evs.column_dimensions[col].width=w
    evid=wb.create_sheet('evidence_samples'); eh=list(evidence[0].keys()) if evidence else ['note']; evid.append(eh)
    for e in evidence[:2000] or [{'note':'No evidence'}]: evid.append([e.get(h,'') for h in eh])
    for cell in evid[1]: cell.font=Font(bold=True); cell.fill=PatternFill('solid', fgColor='D9EAD3')
    wb.save(WORK/'event_comment_feedback_full_table.xlsx')
    emit('report_written', **summary, progress_percent=100, progress_label='таблица готова')

def main():
    init_status()
    try:
        emit('kernel_started', schema=RUN_SCHEMA_VERSION, progress_percent=1, progress_label='запуск Kaggle')
        acquire_status_resources()
        ensure_deps(); load_secrets()
        config_path=find_input_file('run_config.json')
        manifest_path=find_input_file('prod_source_manifest_full.json.gz')
        phrase_path=find_input_file('phrase-bank-v1.md')
        emit('input_files_found', run_config=str(config_path), manifest=str(manifest_path), phrase_bank=str(phrase_path), progress_percent=4, progress_label='payload найден')
        config=load_json_any(config_path); manifest=load_json_any(manifest_path); phrases=parse_phrase_bank(phrase_path)
        max_comments=int(config.get('max_comments_per_source') or 60); sleep_s=float(config.get('request_sleep') or 0.18)
        selected, skipped=build_fetch_posts(manifest); tg=[p for p in selected if p['platform']=='telegram']; vk=[p for p in selected if p['platform']=='vk']
        emit('preflight_ok', events=manifest.get('event_count'), source_posts=manifest.get('source_post_count'), selected=len(selected), tg=len(tg), vk=len(vk), skipped=len(skipped), models=REQUIRED_MODELS, embedding_api_allowed=False, read_mode='human_like_api_paced_v1', request_sleep=sleep_s, progress_percent=8, progress_label=f'источники: TG {len(tg)} / VK {len(vk)}')
        tg_comments,tg_errors,tg_account=asyncio.run(fetch_tg(tg,max_comments,sleep_s)) if tg else ([],[],{})
        vk_comments,vk_errors=fetch_vk(vk,max_comments,sleep_s) if vk else ([],[])
        comments=tg_comments+vk_comments; errors=tg_errors+vk_errors
        (WORK/'comments_raw_redacted.json').write_text(json.dumps({'comments':comments,'errors':errors,'telegram_account':tg_account}, ensure_ascii=False, indent=2), encoding='utf-8')
        emit('fetch_done', comments=len(comments), errors=len(errors), tg_comments=len(tg_comments), vk_comments=len(vk_comments), progress_percent=40, progress_label=f'комментарии: {len(comments)}')
        event_results,evidence=score(comments, manifest, phrases) if comments else ({}, [])
        write_reports(manifest, comments, errors, event_results, evidence)
        emit('kernel_done', progress_percent=100, progress_label='готово')
    except Exception as exc:
        emit('kernel_failed', error_type=type(exc).__name__, message=str(exc)[:1000], progress_label='ошибка')
        # Mark terminal state in the shared status ledger: terminal events are report_written/render_done.
        _status_event('report_written', payload={'phase': 'failed', 'progress_label': 'ошибка'}, status='failed', message=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        finish_status()
if __name__=='__main__': main()
