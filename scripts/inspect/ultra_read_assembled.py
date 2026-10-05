"""Read only already captured public-album text; no credentials or network."""
import argparse,json
from ultra_component_lab import root_path
p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();root=root_path(a.root)
rows=[]
for f in sorted((root/'assembled').glob('*.json')):
 d=json.loads(f.read_text());rows.append({'file':f.name,'source_group':d['source_group'],'domain':d['domain'],
  'bounds_observed':d['bounds_observed'],'evidence_errors':d['evidence_errors'],
  'messages':[{'id':m['id'],'published_at':m['published_at'],'text':m['text']} for m in d['messages'] if m['text']]})
print(json.dumps(rows,ensure_ascii=False,indent=2))
