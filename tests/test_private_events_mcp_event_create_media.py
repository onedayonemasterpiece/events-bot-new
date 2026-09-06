from dataclasses import replace
import time

from aiohttp import web
import pytest

from private_events_mcp.event_create import EventCreateRuntime, parse_event_images
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.repository import InvalidArgumentsError
from private_events_mcp.tool_catalog import ToolExecutionError
from test_private_events_mcp_event_asset_tools import owner
from test_private_events_mcp_event_create_recovery import setup, allowed
from test_private_events_mcp_media_store import make_store


def arguments(media=None):
    result = {'raw_text': 'Лекция о городе в музейном зале, 12 октября в 19:00.',
              'source_external_id': 'poster-event-one', 'idempotency_key': 'poster-key-one'}
    if media is not None:
        result['media'] = media
    return result


@pytest.mark.parametrize('media', [True, {}, [None], [{'asset_ref': 'x', 'content_digest': 'bad'}],
    [{'asset_ref': 'ing_' + 'a'*24, 'content_digest': 'sha256:' + 'b'*64, 'url': 'secret'}]])
def test_reject_unbounded_or_untyped_media(media):
    with pytest.raises(InvalidArgumentsError):
        parse_event_images(media)


@pytest.mark.asyncio
async def test_digest_and_recovery_preserve_media_without_changing_legacy(tmp_path, config):
    database, runtime, executor, original = await setup(tmp_path)
    context = owner(config)
    plain = EventCreateRuntime.request_from_arguments(arguments(), context)
    empty = EventCreateRuntime.request_from_arguments(arguments([]), context)
    assert plain.action_digest == empty.action_digest
    media = [{'asset_ref': 'ing_' + 'a'*24, 'content_digest': 'sha256:' + 'b'*64}]
    image_request = EventCreateRuntime.request_from_arguments(arguments(media), context)
    assert image_request.action_digest != plain.action_digest
    operation, _ = await runtime.store.reserve(image_request)
    restarted = EventCreateRuntime(config=config, database=database, executor=executor)
    assert await restarted.recover_queued(authorize=allowed) == 1
    await restarted.wait_for_operation(operation['operation_ref'])
    assert executor.requests[0].media == image_request.media
    assert executor.requests[0].action_digest == image_request.action_digest


@pytest.mark.asyncio
async def test_real_attachment_prepare_worker_reverify_and_parser_handoff(tmp_path, config, monkeypatch):
    import main
    database, _, _, _ = await setup(tmp_path)
    store, fetcher = make_store(tmp_path / 'media', clock=time.time)
    config = replace(config, event_create_enabled=True, event_assets_enabled=True)
    server = attach_private_events_mcp(web.Application(), config, asset_ingestor=store, event_database=database)
    context = owner(config)
    image = await server.protocol.by_name['event_asset_stage'].handler(
        {'file': {'download_url': 'https://media.example.test/file', 'file_id': 'poster'}}, context)
    args = arguments([{'asset_ref': image['asset_ref'], 'content_digest': image['content_digest']}])
    prepare = await server.protocol.by_name['event_create_prepare'].handler(args, context)
    assert prepare['preview']['media'] == args['media']
    changed = arguments([{'asset_ref': image['asset_ref'], 'content_digest': 'sha256:' + '0'*64}])
    with pytest.raises(ToolExecutionError, match='Event image'):
        await server.protocol.by_name['event_create_prepare'].handler(changed, context)

    seen = []
    async def parser(*unused, **kwargs):
        seen.append(kwargs)
        assert kwargs['creator_id'] is None
        assert kwargs['poster_media'] is None
        assert kwargs['media'][0][0].startswith(b'\x89PNG')
        assert kwargs['media'][0][1].endswith('.png')
        raise main.EventSourceRequiresExactlyOneEvent('fixture stops before canonical write')
    monkeypatch.setattr(main, 'add_events_from_text', parser)
    operation = await server.protocol.by_name['event_create_commit'].handler(
        {**args, 'preparation_ref': prepare['preparation_ref'], 'action_digest': prepare['action_digest']}, context)
    await server.event_create_runtime.wait_for_operation(operation['operation_ref'])
    assert len(seen) == 1 and len(fetcher.calls) == 1
    # Current flag revocation is checked at the durable byte-read boundary.
    request = EventCreateRuntime.request_from_arguments(args, context)
    server.config = replace(server.config, event_assets_enabled=False)
    result = await server.event_create_runtime.executor.create(request)
    assert result['status'] == 'rejected' and result['error_code'] == 'EVENT_ASSETS_DISABLED'
    assert len(seen) == 1
    await server.event_create_runtime.shutdown()
