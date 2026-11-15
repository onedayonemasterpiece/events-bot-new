from __future__ import annotations

import importlib.util
import inspect
import json
import os
import sqlite3
from pathlib import Path


def test_first_publication_uses_changed_anchors_and_keeps_capped_work(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        'related_anchor_scope', os.getenv('RELATED_EXPORTER_PATH') or
        Path(__file__).resolve().parents[1] / 'site/scripts/export-production-preview-data.py')
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    monkeypatch.setenv('GOOGLE_API_KEY4', 'test-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    db = sqlite3.connect(tmp_path / 'catalog.sqlite')
    db.execute('CREATE TABLE event(id INTEGER PRIMARY KEY, title TEXT)')
    db.executemany('INSERT INTO event VALUES (?, ?)',
                   [(i, f'Программа {i}') for i in range(1, 725)])
    db.commit()

    def catalog():
        return [dict(id=row[0], title=row[1], start_date='2026-11-20',
                     lifecycle_status='active', ticket={})
                for row in db.execute('SELECT id, title FROM event ORDER BY id')]

    cache_path = tmp_path / 'cache.json'
    old = catalog()
    cache_path.write_text(json.dumps({
        'schema_version': exporter.PGVECTOR_RELATED_CACHE_SCHEMA_VERSION,
        'algorithm': exporter.PGVECTOR_RELATED_ALGORITHM,
        'embedding_model': 'gemini-embedding-2',
        'embedding_document_version': 'related_v1',
        'event_ids': [event['id'] for event in old],
        'event_fingerprints': {str(event['id']): exporter.event_fingerprint(event) for event in old},
        'chains': {str(event['id']): [] for event in old},
        'raw_chains': {str(event['id']): [] for event in old},
        'gemma_verified_event_ids': list(range(1, 118)),
        'gemma_verification': {'changed_event_ids': list(range(1, 725))},
    }))
    retrieval, audits = [], []

    def rpc(_name, payload, **_kwargs):
        anchor = payload['p_anchor_event_id']
        retrieval.append(anchor)
        return [{'event_id': 1 if anchor != 1 else 2, 'vector_similarity': .85}]

    def audit(**kwargs):
        audits.append(kwargs['anchor']['id'])
        # The old peer's complete facts must survive the separate anchor scope.
        peer = kwargs['candidates'][0]
        assert peer['title'] == db.execute('SELECT title FROM event WHERE id=?', (peer['id'],)).fetchone()[0]
        return {event['id']: {'llm_semantic_score': .9, 'confidence': .9,
                'reject': False, 'similarity_class': 'highly_similar'}
                for event in kwargs['candidates']}

    monkeypatch.setattr(exporter, 'personalization_supabase_request', rpc)
    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)

    def run(cap=20, anchors=None):
        kwargs = dict(current_date='2026-11-15', related_mode='pgvector',
                      cache_path=cache_path, gemma_verify=True, gemma_max_anchors=cap)
        if 'anchor_ids' in inspect.signature(exporter.build_related).parameters:
            kwargs['anchor_ids'] = set(anchors or [])
        return exporter.build_related(catalog(), **kwargs)

    db.execute("INSERT INTO event VALUES (725, 'Новая программа')")
    db.commit()
    result = run()
    assert retrieval == audits == [725]
    assert result['related']['725']['strict_verified']
    assert result['related']['725']['similar'] == [1]
    assert result['related']['1']['strict_verified']
    assert not result['related']['724']['strict_verified']
    warm = run()
    assert warm["gemma_verification"]["status"] == "partial"
    assert retrieval == audits == [725], 'unchanged publication must not repeat retrieval/audit'

    db.executemany('INSERT INTO event VALUES (?, ?)',
                   [(i, f'Новая программа {i}') for i in (726, 727, 728)])
    db.commit()
    partial = run(cap=2)
    assert len(audits) == 3
    saved = json.loads(cache_path.read_text())
    remainder = [event['id'] for event in catalog()
                 if saved['event_fingerprints'].get(str(event['id'])) != exporter.event_fingerprint(event)]
    assert len(remainder) == 1
    assert not partial['related'][str(remainder[0])]['strict_verified']
    db.execute("INSERT INTO event VALUES (729, 'Ещё новая программа')")
    db.commit()
    final = run(cap=2)
    assert len(audits) == 5
    assert all(final['related'][str(i)]['strict_verified'] for i in (726, 727, 728, 729))
    saved = json.loads(cache_path.read_text())
    assert all(saved['event_fingerprints'].get(str(event['id'])) == exporter.event_fingerprint(event) for event in catalog())
    run(cap=2)
    assert len(audits) == 5
    db.execute("UPDATE event SET title='Изменённая программа' WHERE id=1")
    db.commit()
    edited = run(cap=1)
    assert not edited['related']['725']['strict_verified'], 'changed peer invalidates stale verdict'
    completed = run()
    assert all(completed['related'][str(i)]['strict_verified'] for i in (1, 725, 726, 727, 728, 729))
    assert completed['related']['117']['strict_verified']
    # A policy edit cannot relabel stale old verdicts as globally current.
    before_policy_calls = len(audits), len(retrieval)
    signature = exporter.gemma_related_policy_signature
    monkeypatch.setattr(exporter, 'gemma_related_policy_signature',
                        lambda **kwargs: signature(**kwargs) + '-changed')
    new_policy = run(anchors={729})
    assert (len(audits), len(retrieval)) == (before_policy_calls[0] + 1, before_policy_calls[1] + 1)
    assert new_policy['related']['729']['strict_verified']
    assert not new_policy['related']['725']['strict_verified']
    assert not new_policy['strict_verified_related']
    cache_path.unlink()
    before_calls = len(audits), len(retrieval)
    cold_selected = run(anchors={729})
    assert (len(audits), len(retrieval)) == (before_calls[0] + 1, before_calls[1] + 1)
    assert cold_selected['related']['729']['strict_verified']
    assert not cold_selected['related']['117']['strict_verified']
    db.close()


def test_initial_publication_scope_includes_captcha_and_excludes_terminal_or_published(tmp_path, monkeypatch):
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    from scripts.run_static_site_builder_kaggle import related_publication_anchor_ids
    path = tmp_path / 'status.sqlite'
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE event(id INTEGER, vk_source_hash TEXT, lifecycle_status TEXT,
                               silent INTEGER, photo_urls TEXT, source_vk_post_url TEXT);
            CREATE TABLE joboutbox(event_id INTEGER, task TEXT, status TEXT,
                                   terminal_reason TEXT, last_error TEXT);
        """)
        db.executemany('INSERT INTO event VALUES (?, ?, ?, ?, ?, ?)', [
            (1, None, 'active', 0, '["poster"]', None),
            (2, 'published-own-hash', 'active', 0, '["poster"]', None),
            (3, None, 'active', 0, '["poster"]', None),
            (4, None, 'active', 0, '[]', None),
            (5, None, 'active', 0, '["poster"]', 'https://vk.ru/wall-999_1'),
            (6, None, 'active', 0, '["poster"]', 'https://vk.ru/wall-231920894_14511'),
        ])
        db.executemany('INSERT INTO joboutbox VALUES (?, ?, ?, ?, ?)', [
            (1, 'vk_sync', 'pending', None, 'captcha_wait:1794739815'),
            (2, 'vk_sync', 'pending', None, None),
            (3, 'vk_sync', 'error', 'corruption_contained', 'lost queue row'),
            (4, 'vk_sync', 'pending', None, None),
            (5, 'vk_sync', 'pending', None, None),
            (6, 'vk_sync', 'pending', None, None),
        ])
    assert related_publication_anchor_ids(str(path)) == [1, 5]
