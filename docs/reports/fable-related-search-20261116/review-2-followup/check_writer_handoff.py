"""Exercise the two real writer entry points using in-memory storage and deterministic embeddings."""
import contextlib
import copy
import importlib.util
import io
import json
import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import parse_qs

from scripts import sync_event_search_vectors_to_supabase as new

if len(sys.argv) != 2:
    raise SystemExit("usage: check_writer_handoff.py previous_sync.py")
spec = importlib.util.spec_from_file_location("previous_sync", sys.argv[1])
old = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = old
spec.loader.exec_module(old)
temporary = tempfile.TemporaryDirectory()
work = Path(temporary.name)
docs = {}
vectors = {}
writes = []
checks = []

def rest(method, path, *, body=None, **_):
    table, _, raw = path.partition('?')
    query = {k:v[0] for k,v in parse_qs(raw).items()}
    rows = docs if table == 'event_search_documents' else vectors
    if method == 'POST':
        for row in body:
            key = row['event_id'] if rows is docs else (row['event_id'],row['embedding_doc_kind'])
            rows[key] = copy.deepcopy(row)
        writes.append((method,table))
        return []
    matches = list(rows.values())
    if 'embedding_doc_kind' in query:
        matches = [r for r in matches if r['embedding_doc_kind']==query['embedding_doc_kind'][3:]]
    if 'event_id' in query:
        ids = {int(i) for i in re.findall(r'\d+',query['event_id'])}
        matches = [r for r in matches if r['event_id'] in ids]
    if 'or' in query:
        version = query['or'].split('.neq.')[1].rstrip(')')
        matches = [r for r in matches if r.get('metadata',{}).get('doc_version') != version]
    if method == 'PATCH':
        for row in matches:
            row.update(copy.deepcopy(body))
        writes.append((method,table))
        return []
    assert method=='GET', (method,table)
    return matches

def project(mod, events):
    for key, value in [('supabase_request',rest),('build_embedding_client',lambda **kw: object()),
                       ('gemini_embed',lambda text,**kw:[1.,0.])]:
        setattr(mod,key,value)
    fixture, report = work/'events.json', work/'receipt.json'
    fixture.write_text(json.dumps({'events':events,'build':{'catalog_revision':mod.exported_search_catalog_revision(events)}}))
    sys.argv=['sync','--apply','--require-complete','--prune-missing','--embedding-dim','2',
              '--sleep-seconds','0','--preview-events-json',str(fixture),'--report-json',str(report)]
    with contextlib.redirect_stdout(io.StringIO()):
        assert mod.main()==0
    r=json.loads(report.read_text())
    assert r['complete'] and r['current_embedding_count']==len(events)*2
    return r

def event(i):
    return dict(id=i, title=f'Programme {i}', slug=f'p{i}',start_date='2026-11-18',
                ticket={},lifecycle_status='active',canonical_search_digest='Live theatre.')

rows=[event(1)]
project(old,rows)
before=copy.deepcopy((docs,vectors,writes))
try:
    project(new,[*rows,event(2)])
except RuntimeError as exc:
    assert 'switch the complete index' in str(exc)
else:
    raise AssertionError('new writer accepted old index')
assert before==(docs,vectors,writes)
v2=project(old,[*rows,event(2)])
checks.append(dict(case='cancel_before_commit_then_new_event_on_v2',version=docs[1]['related_doc_version'],
                   docs=v2['current_document_count'],current_embeddings=v2['current_embedding_count'],calls=v2['provider_calls']))

# For the post-COMMIT case, seed the result of a complete v3 index in memory.
# Separate PostgreSQL integration covers the SQL transition itself.
docs.clear()
vectors.clear()
project(new,rows)
pre=copy.deepcopy(vectors)
changed=dict(rows[0],canonical_search_digest='Accepted new fantasy facts.')
post=project(new,[changed,event(2)])
assert pre[1,'search_v3']['text_hash']==vectors[1,'search_v3']['text_hash']
assert pre[1,'related_v1']['text_hash']!=vectors[1,'related_v1']['text_hash']
assert post['provider_calls']==5
checks.append(dict(case='v3_catches_concurrent_digest_edit_and_new_card',version=new.RELATED_DOC_VERSION,
                 docs=post['current_document_count'],current_embeddings=post['current_embedding_count'],
                 changed_related_hash=True,retained_search_hash=True,calls=post['provider_calls']))
assert project(new,[changed,event(2)])['provider_calls']==0
checks.append(dict(case='unchanged_retry',calls=0))
temporary.cleanup()
print(json.dumps(checks,indent=2))
