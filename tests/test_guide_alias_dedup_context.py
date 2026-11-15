"""The actual pair router and judge must receive alias/schedule evidence."""
import ast
import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Mapping


def load_functions():
    path = Path(__file__).parents[1] / 'guide_excursions' / 'dedup.py'
    tree = ast.parse(path.read_text())
    functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.name in {'_candidate_prefilter', '_ask_pair_judge_llm'}]
    namespace = dict(Any=Any, Mapping=Mapping, asyncio=asyncio, json=json,
                     logger=logging.getLogger(__name__), collapse_ws=lambda text: ' '.join(text.split()),
                     GUIDE_EXCURSIONS_DEDUP_MODEL='test', GUIDE_EXCURSIONS_DEDUP_LLM_TIMEOUT_SECONDS=2,
                     _extract_json=json.loads)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), 'exec'), namespace)
    return namespace


def test_same_source_same_slot_reaches_semantic_judge_without_title_overlap():
    route = load_functions()['_candidate_prefilter']
    accepted, _ = route({'same_source': True, 'same_date': True, 'same_time': True,
                         'token_overlap_score': 0, 'shared_route_tokens': []})
    assert accepted
    assert not route({'same_source': True, 'same_date': False, 'same_time': True})[0]


def test_judge_preserves_full_source_and_guide_identity():
    namespace = load_functions()
    captured = {}

    class Client:
        async def generate_content_async(self, **kwargs):
            captured.update(kwargs)
            return json.dumps({'decision': 'distinct'}), {}

    namespace['_get_dedup_client'] = lambda: (Client(), [])
    tail = 'РАСПИСАНИЕ: 17 ноября 14:00 Зелёные истории, Ольга Рубцова.'
    source = 'Рассказ о Достоевском. ' * 100 + tail
    row = {'id': 745, 'dedup_source_text': source, 'guide_names': ['Ольга Рубцова'],
           'city': 'Калининград', 'source_post_url': 'https://vk.com/wall41501067_3447'}
    asyncio.run(namespace['_ask_pair_judge_llm'](row, dict(row, id=753), {}))
    payload = json.loads(captured['prompt'].split('Input:\n', 1)[1])
    for side in ['left', 'right']:
        assert payload[side]['post_excerpt'].endswith(tail)
        assert payload[side]['guide_names'] == ['Ольга Рубцова']

        assert payload[side]['city'] == 'Калининград'
        assert payload[side]['source_post_url'] == row['source_post_url']


def test_unknown_place_sentinel_does_not_reach_card_text():
    path = Path(__file__).parents[1] / 'guide_excursions' / 'visual_digest.py'
    tree = ast.parse(path.read_text())
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_plain']
    namespace = dict(Any=Any, collapse_ws=lambda text: ' '.join(text.split()))
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), 'exec'), namespace)
    assert namespace['_plain']('not_specified') == ''
    assert namespace['_plain']('Калининград') == 'Калининград'
