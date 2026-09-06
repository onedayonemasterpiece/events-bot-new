from pathlib import Path
import yaml


def test_hero_talk_ids_extend_existing_registry_without_claiming_acceptance():
    registry = yaml.safe_load((Path(__file__).parents[1] / 'docs/testing/private-events-mcp-event-operations-scenarios.v2.yml').read_text())
    assert registry['schema_version'] == 2
    rows = registry['scenarios']
    ids = [row['id'] for row in rows]
    assert len(ids) == len(set(ids))
    assert {'CRT-001', 'QRY-001', 'LIVE-001'} <= set(ids)
    required = {f'HT-AF-{i:02d}' for i in range(1, 23)} | {f'MCP-HT-{i:02d}' for i in range(1, 7)}
    assert required <= set(ids)
    for row in rows:
        if row['id'] not in required:
            continue
        assert row['implementation_status'] in {'partial', 'missing'}
        assert row['acceptance_status'] == 'not_run'
        for name in row.get('partial_test_files', []):
            assert (Path(__file__).parents[1] / name).is_file()
