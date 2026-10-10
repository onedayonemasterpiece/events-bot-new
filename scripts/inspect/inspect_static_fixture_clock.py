"""Print non-sensitive browser fixture clock and selected specimen dates."""
import json
from pathlib import Path

source = json.loads(Path('site/src/data/preview-events.json').read_text())
print(json.dumps({'build': source.get('build'), 'keys': list(source)}, ensure_ascii=False))
for event in source.get('events', []):
    if str(event.get('id')) in {'6408', '6407', '6399'}:
        print(json.dumps({k: v for k, v in event.items() if k in {'id', 'title', 'date', 'end_date', 'start_at', 'end_at'}}, ensure_ascii=False))
