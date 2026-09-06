from dataclasses import replace

import pytest

from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.event_assets import EventAssetService
from private_events_mcp.media_contract import ChatGPTFile
from private_events_mcp.tool_catalog import ToolCallContext, ToolExecutionError
from test_private_events_mcp_media_store import make_store


def context(subject='partner:one', client='client-one', resource='https://events.test/partner'):
    return ToolCallContext(AccessIdentity(subject, client, frozenset({'partner:events:write'}),
                                         resource, 'token', 5000), resource)


async def allowed(ctx, action):
    assert action in {'stage', 'read', 'use'}
    return True


def service(store, **kwargs):
    return EventAssetService(ingestor=store, binding_key='k' * 32,
                             authorize=kwargs.pop('authorize', allowed),
                             clock=kwargs.pop('clock', lambda: 1000), **kwargs)


def file():
    return ChatGPTFile('https://media.example.test/download', 'file-one', 'image/png', 'poster.png')


@pytest.mark.asyncio
async def test_real_store_restart_and_internal_verified_stream(tmp_path):
    store, fetcher = make_store(tmp_path)
    first = service(store)
    staged = await first.stage(file(), context())
    assert len(fetcher.calls) == 1
    assert set(staged) == {'asset_ref', 'content_digest', 'mime_type', 'byte_length',
                           'width', 'height', 'expires_at', 'role'}
    second_store, unused_fetcher = make_store(tmp_path)
    second = service(second_store)
    assert await second.read(staged['asset_ref'], context()) == staged
    verified = await second.reverify(staged['asset_ref'], context(), expected_digest=staged['content_digest'])
    with second_store.open_verified(verified.storage_ref, verified.owner_binding) as (stream, metadata):
        assert len(stream.read()) == staged['byte_length']
        assert metadata.content_digest == staged['content_digest']
    assert not unused_fetcher.calls


@pytest.mark.asyncio
@pytest.mark.parametrize('other', [context(subject='partner:two'), context(client='other'),
                                   context(resource='https://events.test/owner')])
async def test_cross_principal_client_resource_denied(tmp_path, other):
    store, _ = make_store(tmp_path)
    runtime = service(store)
    staged = await runtime.stage(file(), context())
    with pytest.raises(ToolExecutionError):
        await runtime.read(staged['asset_ref'], other)


@pytest.mark.asyncio
async def test_revoke_prevents_stage_read_and_use(tmp_path):
    active = True

    async def policy(ctx, action):
        return active

    store, fetcher = make_store(tmp_path)
    runtime = service(store, authorize=policy)
    staged = await runtime.stage(file(), context())
    active = False
    for operation in (runtime.stage(file(), context()), runtime.read(staged['asset_ref'], context()),
                      runtime.reverify(staged['asset_ref'], context(), expected_digest=staged['content_digest'])):
        with pytest.raises(ToolExecutionError):
            await operation
    assert len(fetcher.calls) == 1


@pytest.mark.asyncio
async def test_expired_refs_not_extended_by_restart(tmp_path):
    now = [1000]
    store, _ = make_store(tmp_path, clock=lambda: now[0])
    staged = await service(store, ttl_seconds=60, clock=lambda: now[0]).stage(file(), context())
    now[0] = staged['expires_at']
    store2, _ = make_store(tmp_path, clock=lambda: now[0])
    with pytest.raises(ToolExecutionError):
        await service(store2, clock=lambda: now[0]).read(staged['asset_ref'], context())


@pytest.mark.asyncio
async def test_wrong_digest_and_modified_bytes_fail(tmp_path):
    store, _ = make_store(tmp_path)
    runtime = service(store)
    staged = await runtime.stage(file(), context())
    with pytest.raises(ToolExecutionError):
        await runtime.reverify(staged['asset_ref'], context(), expected_digest='sha256:' + '0' * 64)
    row = store._row_for(staged['asset_ref'])
    path = store._root / row['filename']
    assert path.is_file()
    path.chmod(0o600)  # simulate disk tampering despite immutable normal storage
    path.write_bytes(b'tampered bytes')
    path.chmod(0o400)
    with pytest.raises(ToolExecutionError):
        await runtime.read(staged['asset_ref'], context())


@pytest.mark.asyncio
async def test_revoked_during_ingestion_not_returned(tmp_path):
    calls = []

    async def policy(ctx, action):
        calls.append(action)
        return len(calls) == 1

    store, _ = make_store(tmp_path)
    with pytest.raises(ToolExecutionError):
        await service(store, authorize=policy).stage(file(), context())
    assert calls == ['stage', 'stage']


@pytest.mark.asyncio
async def test_invalid_ingestor_result_and_policy_exception_denied(tmp_path):
    store, fetcher = make_store(tmp_path)

    async def broken_policy(ctx, action):
        raise RuntimeError('secret should not leak')

    with pytest.raises(ToolExecutionError) as error:
        await service(store, authorize=broken_policy).stage(file(), context())
    assert 'secret' not in str(error.value)
    assert not fetcher.calls
    real_ingest = store.ingest

    async def document(*args, **kwargs):
        return replace(await real_ingest(*args, **kwargs), role='document')

    store.ingest = document
    with pytest.raises(ToolExecutionError):
        await service(store).stage(file(), context())


@pytest.mark.asyncio
async def test_context_resource_must_match_identity(tmp_path):
    store, fetcher = make_store(tmp_path)
    ctx = replace(context(), resource='https://events.test/owner')
    with pytest.raises(ToolExecutionError):
        await service(store).stage(file(), ctx)
    assert not fetcher.calls


def test_callback_and_limits_required(tmp_path):
    store, _ = make_store(tmp_path)
    for kwargs in ({'authorize': None}, {'ttl_seconds': 86401}, {'max_bytes': True}):
        with pytest.raises(ValueError):
            service(store, **kwargs)


async def durable_policy():
    return True


def durable(runtime, staged, ctx=None, **kwargs):
    who = (ctx or context()).identity
    return runtime.read_durable(staged['asset_ref'], expected_digest=staged['content_digest'],
                                actor_subject=who.subject, actor_client_id=who.client_id,
                                actor_audience=who.audience,
                                authorize=kwargs.pop('authorize', durable_policy), **kwargs)


@pytest.mark.asyncio
async def test_durable_read_restart_uses_actor_fields_not_oauth_token(tmp_path):
    import hashlib
    store, _ = make_store(tmp_path)
    staged = await service(store).stage(file(), context())
    reopened, fetcher = make_store(tmp_path)

    async def no_http_authorization(*args):
        raise AssertionError('durable execution must not fabricate an HTTP identity')

    runtime = service(reopened, authorize=no_http_authorization)
    content, name = await durable(runtime, staged)
    assert 'sha256:' + hashlib.sha256(content).hexdigest() == staged['content_digest']
    assert name == 'event-image-' + staged['content_digest'][7:23] + '.png'
    assert not fetcher.calls


@pytest.mark.asyncio
@pytest.mark.parametrize('deny_at', [1, 2, 3, 4])
async def test_durable_policy_checked_around_each_io_phase(tmp_path, deny_at):
    store, _ = make_store(tmp_path)
    runtime = service(store)
    staged = await runtime.stage(file(), context())
    checks = []

    async def policy():
        checks.append(True)
        return len(checks) < deny_at

    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged, authorize=policy)
    assert len(checks) == deny_at


@pytest.mark.asyncio
async def test_durable_expiry_and_wrong_actor_fail(tmp_path):
    now = [1000]
    store, _ = make_store(tmp_path, clock=lambda: now[0])
    runtime = service(store, clock=lambda: now[0], ttl_seconds=60)
    staged = await runtime.stage(file(), context())
    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged, context(subject='partner:other'))
    now[0] = staged['expires_at']
    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged)


@pytest.mark.asyncio
async def test_durable_hashes_actual_stream_bytes_and_bounds_read(tmp_path):
    from contextlib import contextmanager
    from io import BytesIO
    store, _ = make_store(tmp_path)
    runtime = service(store)
    staged = await runtime.stage(file(), context())
    real_open = store.open_verified
    sizes = []

    class CorruptStream(BytesIO):
        def read(self, size):
            sizes.append(size)
            return super().read(size)

    @contextmanager
    def corrupt(ref, binding):
        with real_open(ref, binding) as (stream, metadata):
            yield CorruptStream(b'x' * metadata.byte_length), metadata

    store.open_verified = corrupt
    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged)
    assert sizes == [runtime.max_bytes + 1]


@pytest.mark.asyncio
async def test_durable_revoke_during_stream_io_and_expiry_after_read(tmp_path):
    from contextlib import contextmanager
    now = [1000]
    store, _ = make_store(tmp_path, clock=lambda: now[0])
    runtime = service(store, clock=lambda: now[0], ttl_seconds=60)
    staged = await runtime.stage(file(), context())
    active = [True]
    real_open = store.open_verified

    @contextmanager
    def revoking(ref, binding):
        with real_open(ref, binding) as pair:
            yield pair
        active[0] = False

    async def policy():
        return active[0]

    store.open_verified = revoking
    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged, authorize=policy)

    @contextmanager
    def expiring(ref, binding):
        with real_open(ref, binding) as pair:
            yield pair
        now[0] = staged['expires_at']

    store.open_verified = expiring
    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged)


@pytest.mark.asyncio
async def test_durable_requires_current_policy_and_verified_opener(tmp_path):
    store, _ = make_store(tmp_path)
    runtime = service(store)
    staged = await runtime.stage(file(), context())
    with pytest.raises(ValueError):
        await durable(runtime, staged, authorize=None)
    store.open_verified = None
    with pytest.raises(ToolExecutionError):
        await durable(runtime, staged)
