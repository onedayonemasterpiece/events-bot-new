"""Reproduce the bounded Gemma study from inside the deployed application container.
Default: prepare only. With --execute, each of twelve slots permits one provider send.
Write stdout to an experiment artifact on the operator's machine, outside production.
"""
import collections
import base64
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import zlib
from datetime import datetime, timezone

# Pre-registered before results. The first ten are the frozen equal-mix collector.
# Two Potter cards removed for 11068; two low-rank neighbours enter for stand-up;
# two Potter cards enter for Canterville. All eight shared cards keep their slots.
ARMS = {
    11068: ([10443, 9794, 10607, 10721, 10416, 10915, 10798, 10883, 10860, 6658],
            {0: 10756, 4: 10689}),
    11010: ([10939, 10938, 10976, 10940, 10960, 10496, 11083, 9804, 9708, 9107],
            {4: 10639, 5: 10544}),
    10580: ([10915, 10598, 10607, 10578, 10606, 11002, 10507, 10998, 10720, 10441],
            {4: 10416, 5: 10443}),
}
ORDER = [
    (11068, 'A1'), (11010, 'A1'), (10580, 'A1'),
    (11068, 'B'),  (11010, 'A2'), (10580, 'A2'),
    (11068, 'A2'), (11010, 'B'),  (10580, 'A3'),
    (11068, 'A3'), (11010, 'A3'), (10580, 'B'),
]
MODEL = 'models/gemma-4-26b-a4b-it'
POLICY = 'bfc70cddbe4803d472676dbe7614985fac665fee'
DATE = '2026-11-16'


def sha(value):
    return hashlib.sha256(value).hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat()


def emit(kind, **values):
    print(json.dumps(dict(fable17=kind, utc=utc(), **values), ensure_ascii=False), flush=True)


if '--make-resume' in sys.argv:
    # Run this on the operator host. stdout is a script for python - [--execute].
    # No runtime, key, DB or network access is needed to build or inspect the plan.
    argv = sys.argv[sys.argv.index('--make-resume')+1:]
    selected = None
    if '--slots' in argv:
        idx = argv.index('--slots')
        selected = {int(v) for v in argv[idx+1].split(',')}
        del argv[idx:idx+2]
    skip_started = set()
    if '--skip-started' in argv:
        idx = argv.index('--skip-started')
        for journal in argv[idx+1:]:
            entries = [json.loads(l) for l in Path(journal).read_text().splitlines() if l.strip()]
            assert entries and entries[0]['model'] == MODEL and entries[0]['policy'] == POLICY
            skip_started.update(r['slot'] for r in entries if r['fable17']=='start')
        argv = argv[:idx]
    records = [json.loads(line) for line in Path(argv[0]).read_text().splitlines() if line.strip()]
    assert records[-1]['fable17'] == 'finished'
    frozen = json.loads(Path(argv[1]).read_text()) if len(argv)>1 else [r for r in records if r['fable17']=='frozen']
    assert len(frozen)==6 and all(sha(r['prompt'].encode()) == r['sha256'] for r in frozen)
    physical = [r for r in records if r['fable17'] in ('reply','error','interrupted_sent') and r['sends']]
    latest = {r['slot']:r for r in records if r['fable17'] in ('start','reply','error','interrupted_sent')}
    succeeded = {r['slot'] for r in physical if r['fable17']=='reply'}
    assert len(latest)==len(ORDER)
    if selected is None:
        assert len(physical)==len(ORDER)
    eligible = {s for s,r in latest.items() if r['fable17']=='error' and r['sends'] and r['status_code']==503}
    eligible -= succeeded | skip_started
    assert selected is None or selected <= eligible, 'Selected slot was successful, in flight, or lacks a confirmed 503'
    targets = sorted(eligible if selected is None else selected)
    assert 0<len(targets)<=6 and [list(pair) for pair in ORDER] == records[0]['order']
    configs = {r['config_sha256'] for r in physical if 'config_sha256' in r}
    assert len(configs)==1
    bundle = dict(frozen=frozen, targets=targets, config_sha256=configs.pop(), design=records[0])
    blob = base64.b64encode(zlib.compress(json.dumps(bundle,ensure_ascii=False).encode(),9)).decode()
    print('import base64,json,zlib\nRESUME=json.loads(zlib.decompress(base64.b64decode('+repr(blob)+')))')
    print(Path(__file__).read_text())
    raise SystemExit(0)

resume = globals().get('RESUME')
if '--show-plan' in sys.argv:
    assert resume
    for slot in resume['targets']:
        anchor,label=ORDER[slot-1]
        r=next(x for x in resume['frozen'] if x['anchor']==anchor and x['arm']==label[0])
        assert sha(r['prompt'].encode())==r['sha256']
        print(json.dumps(dict(slot=slot,anchor=anchor,label=label,ids=r['ids'],prompt_sha256=r['sha256'],
                              model=MODEL,config_sha256=resume['config_sha256'])))
    raise SystemExit(0)
path = Path('site/scripts/export-production-preview-data.py').resolve()
sys.path.insert(0, str(path.parent))
spec = importlib.util.spec_from_file_location('fable_repeat_exporter', path)
exporter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = exporter
spec.loader.exec_module(exporter)
exporter.SKIP_IMAGE_PROBES = True
from google_ai.client import GoogleAIClient

default = inspect.signature(exporter.build_related).parameters
model = default['gemma_model'].default
key_env = default['gemma_key_env'].default
policy = exporter.gemma_related_policy_signature(model=model)
assert model == MODEL and policy == POLICY, (model, policy)
assert os.getenv(key_env) and os.getenv('GOOGLE_AI_NORMAL_KEY_ENVS')
fact_chars = max(120, int(os.getenv('STATIC_SITE_GEMMA_RELATED_FACT_MAX_CHARS') or 360))
all_ids = sorted({item for anchor, (ids, repl) in ARMS.items() for item in (anchor, *ids, *repl.values())})
if resume:
    assert resume['design']['exporter_sha256'] == sha(path.read_bytes())
    assert resume['design']['client_sha256'] == sha(Path(inspect.getfile(GoogleAIClient)).read_bytes())
    captured = {(r['anchor'],tuple(r['ids'])):r for r in resume['frozen']}
    def saved_prompt(*, anchor, candidates, fact_max_chars):
        r = captured[anchor['id'],tuple(p['id'] for p in candidates)]
        assert sha(r['prompt'].encode()) == r['sha256']
        return r['prompt']
    exporter.build_gemma_related_audit_prompt = saved_prompt
    events = {i:{'id':i} for i in all_ids}
else:
    db = sqlite3.connect('file:/data/db.sqlite?mode=ro', uri=True)
    db.execute('pragma query_only=ON')
    db.row_factory = sqlite3.Row
    query = 'select * from event where id in (' + ','.join('?' for _ in all_ids) + ')'
    events = {row['id']: exporter.build_event(db, row, DATE, participants=[], video_assets=[]) for row in db.execute(query, all_ids)}
    db.close()
assert set(events) == set(all_ids)
packets = {}
for anchor, (ids, changes) in ARMS.items():
    for arm, peers in [('A', ids), ('B', [changes.get(i, p) for i, p in enumerate(ids)])]:
        assert len(set(peers)) == 10
        args = dict(anchor=events[anchor], candidates=[events[x] for x in peers])
        prompt = exporter.build_gemma_related_audit_prompt(**args, fact_max_chars=fact_chars)
        packets[anchor, arm] = (peers, args, prompt)

emit('design', model=model, policy=policy, date=DATE, exporter_sha256=sha(path.read_bytes()),
     client_sha256=sha(Path(inspect.getfile(GoogleAIClient)).read_bytes()), fact_chars=fact_chars,
     pool_size=len(os.environ['GOOGLE_AI_NORMAL_KEY_ENVS'].split(',')),
     order=ORDER, threshold=0.72, resume_slots=resume['targets'] if resume else [],
     prepare_only='--execute' not in sys.argv)
for (anchor, arm), (peers, args, prompt) in packets.items():
    emit('frozen', anchor=anchor, arm=arm, ids=peers, sha256=sha(prompt.encode()), prompt=prompt)

if '--execute' not in sys.argv:
    raise SystemExit(0)

original_generate = GoogleAIClient.generate_content_async
original_reserve = GoogleAIClient._reserve
original_provider = GoogleAIClient._call_provider
original_parse = exporter.parse_gemma_json_object


async def record_reservation(self, *args, **kwargs):
    result = await original_reserve(self, *args, **kwargs)
    fields = ('ok', 'blocked_reason', 'retry_after_ms', 'rpm_limit', 'tpm_limit', 'rpd_limit',
              'used_rpm', 'used_tpm', 'used_rpd', 'limits', 'used_after', 'limiter_contract', 'bucket_strategy')
    observed['reservations'].append({k: getattr(result, k) for k in fields if getattr(result, k, None) is not None})
    return result


async def record_provider(self, *args, **kwargs):
    observed['sends'] += 1
    assert observed['sends'] == 1
    return await original_provider(self, *args, **kwargs)


async def record_generate(self, **kwargs):
    # Built-in supported single-send option disables transparent SDK retries/429 rotation.
    # Admission, credentials, selected model, payload and accounting stay on the normal client.
    assert 'max_provider_attempts' in inspect.signature(original_generate).parameters
    self.hard_single_provider_attempt = True
    self.allow_provider_429_rotation = False
    assert kwargs['prompt'] == prompt and kwargs['model'] == MODEL
    config = kwargs['generation_config']
    assert config['temperature'] == 0.1
    observed['config_sha256'] = sha(json.dumps(config, sort_keys=True, ensure_ascii=False).encode())
    if resume:
        assert observed['config_sha256'] == resume['config_sha256']
    observed['config'] = config
    txt, usage = await original_generate(self, max_provider_attempts=1, **kwargs)
    observed['reply_sha256'] = sha(txt.encode())
    usage_fields = ('prompt_tokens', 'completion_tokens', 'total_tokens', 'thoughts_tokens')
    observed['usage'] = {k: getattr(usage, k) for k in usage_fields if getattr(usage, k, None) is not None}
    return txt, usage


def record_parse(txt):
    parsed = original_parse(txt)
    rows = parsed.get('ranked') or []
    observed['emitted_ids'] = [r.get('event_id') if isinstance(r, dict) else None for r in rows]
    observed['raw_rows'] = [{k:r.get(k) for k in ('event_id','llm_semantic_score','reject','confidence','similarity_class')}
                            for r in rows if isinstance(r,dict)]
    return parsed


GoogleAIClient._reserve = record_reservation
GoogleAIClient._call_provider = record_provider
GoogleAIClient.generate_content_async = record_generate
exporter.parse_gemma_json_object = record_parse

for slot, (anchor, label) in enumerate(ORDER, 1):
    if resume and slot not in resume['targets']:
        continue
    peers, args, prompt = packets[anchor, label[0]]
    packet_key = dict(slot=slot, anchor=anchor, label=label, ids=peers, packet_sha256=sha(prompt.encode()))
    for admission_try in range(1, 11):
        observed = dict(sends=0, reservations=[])
        emit('start', **packet_key, admission_try=admission_try)
        try:
            reply = exporter.call_gemma_related_audit(model=model, key_env=key_env,
                      candidate_limit=10, timeout_seconds=60, **args)
        except Exception as error:
            retry_ms = int(getattr(error, 'retry_after_ms', 0) or 0)
            reason = str(getattr(error, 'blocked_reason', '') or '')
            retryable = not observed['sends'] and reason in {'rpm', 'tpm', 'rpd', 'provider_429'}
            emit('error', **packet_key, **observed, admission_try=admission_try,
                 error_type=type(error).__name__, status_code=getattr(error, 'status_code', None),
                 blocked_reason=reason, retry_after_ms=retry_ms, retryable_presend=retryable)
            if retryable and admission_try < 10:
                delay = max(62, 2 + retry_ms / 1000)
                if delay > 900:  # Respect the actual reset rather than continue probing other slots.
                    emit('stopped', reason='long_quota_reset', earliest_retry_seconds=delay)
                    raise SystemExit(3)
                time.sleep(delay)
                continue
            if not observed['sends']:
                emit('stopped', reason='pre_send_failure')
                raise SystemExit(3)
            time.sleep(max(62, 2 + retry_ms / 1000))
            break
        scores = {eid: {k: v.get(k) for k in ('llm_semantic_score','reject','confidence','similarity_class')}
                  for eid,v in reply.items()}
        raw_ids = observed.get('emitted_ids', [])
        counts = collections.Counter(str(i) for i in raw_ids)
        emit('reply', **packet_key, **observed, scores=scores,
             unexpected=[k for k in counts if k not in {str(i) for i in peers}],
             duplicate=[k for k,c in counts.items() if c>1],
             missing=[eid for eid in peers if eid not in reply], admission_try=admission_try)
        break
emit('finished', slots=len(resume['targets']) if resume else len(ORDER))

