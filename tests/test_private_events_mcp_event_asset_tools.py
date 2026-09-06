from dataclasses import replace
import time

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.event_asset_tools import parse_event_file
from private_events_mcp.server import PrivateEventsMCPServer
from private_events_mcp.tool_catalog import ToolCallContext, ToolExecutionError
from test_private_events_mcp_media_store import make_store


def owner(config, **overrides):
    identity = AccessIdentity('events-bot-owner', config.oauth_client_id,
                              frozenset({'events:write'}), config.resource, 'token', int(time.time()) + 300)
    return ToolCallContext(replace(identity, **overrides), config.resource)


def enabled(config):
    return replace(config, event_assets_enabled=True, media_allowed_hosts=('media.example.test',))


def test_default_off_and_missing_storage(config):
    server = PrivateEventsMCPServer(config)
    assert server.event_assets is None
    assert 'event_asset_stage' not in server.protocol.by_name
    with pytest.raises(ValueError, match='secure private storage'):
        PrivateEventsMCPServer(enabled(config))


@pytest.mark.asyncio
async def test_owner_event_ingress_without_social_and_codex_isolation(config, tmp_path):
    store, fetcher = make_store(tmp_path, clock=time.time)
    server = PrivateEventsMCPServer(enabled(config), asset_ingestor=store)
    assert server.social_workspace is None
    assert set(server.codex_protocol.by_name) == {'search', 'fetch', 'event_get', 'events_search', 'operations_snapshot', 'incident_get', 'incidents_search'}
    stage = server.protocol.by_name['event_asset_stage']
    assert stage.descriptor()['_meta']['openai/fileParams'] == ['file']
    data = await stage.handler({'file': {'download_url': 'https://media.example.test/download', 'file_id': 'file-1'}}, owner(config))
    result = await server.protocol.by_name['event_asset_get'].handler({'asset_ref': data['asset_ref']}, owner(config))
    assert result == data and len(fetcher.calls) == 1
    assert set(data) == {'asset_ref', 'content_digest', 'mime_type', 'byte_length', 'width', 'height', 'expires_at', 'role'}
    for changes in ({'client_id': config.codex_oauth_client_id}, {'subject': 'partner:one'},
                    {'scopes': frozenset()}, {'expires_at': 1}, {'audience': config.codex_resource}):
        with pytest.raises(ToolExecutionError):
            await stage.handler({'file': {'download_url': 'https://media.example.test/download', 'file_id': 'file-1'}}, owner(config, **changes))
    assert len(fetcher.calls) == 1
    server.config = replace(server.config, event_assets_enabled=False)
    with pytest.raises(ToolExecutionError):
        await server.event_assets.read(data['asset_ref'], owner(config))


@pytest.mark.parametrize('patch', [
    {'download_url': 'http://files.test/x'}, {'download_url': 'https://u:p@files.test/x'},
    {'download_url': 'https://files.test/x#secret'}, {'download_url': 'https://files.test:bad/x'},
    {'download_url': 'https://[broken/x'}, {'download_url': ' https://files.test/x'},
    {'file_id': ''}, {'file_id': True}, {'file_id': 'x\n'}, {'file_name': '../private'},
    {'mime_type': 'application/pdf'}, {'unknown': 'secret'},
])
def test_bad_nested_descriptors(patch):
    with pytest.raises(ToolExecutionError):
        parse_event_file({'download_url': 'https://files.test/x', 'file_id': 'f', **patch})
