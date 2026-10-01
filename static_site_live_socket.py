"""aiohttp I/O adapter for the canonical shared Live WSS relay.

Product JWT/origin/resource policy stays in static_site_live_search. This file
contains no audio queue, Gemini connection, key selection or protocol fork.
"""
from __future__ import annotations

from aiohttp import WSMsgType, web


def socket_handler(*, host, config, resource_id, origin_check):
    async def handle(request: web.Request) -> web.StreamResponse:
        # Import only for the enabled route; disabled static-site deployments
        # retain their original optional Live dependency behavior.
        from live_interaction import LiveError
        from live_interaction.socket_transport import SOCKET_PROTOCOL, serve_socket, socket_ticket

        if request.query_string or not origin_check(request, config):
            return web.json_response({"error": "origin_or_query_forbidden"}, status=403)
        offered = [value.strip() for value in request.headers.get("Sec-WebSocket-Protocol", "").split(",") if value.strip()]
        session_id = str(request.match_info.get("session_id") or "")
        try:
            ticket = socket_ticket(offered)
            binding = host.open_socket(session_id=session_id, resource_id=resource_id, ticket=ticket)
        except LiveError as exc:
            return web.json_response({"error": exc.code}, status=403)
        websocket = web.WebSocketResponse(protocols=[SOCKET_PROTOCOL], max_msg_size=65536, autoping=True)
        try:
            await websocket.prepare(request)
        except Exception:
            await binding.close()
            raise

        async def receive():
            message = await websocket.receive()
            if message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                return None
            if message.type in (WSMsgType.TEXT, WSMsgType.BINARY):
                return message.data
            return None

        async def send(payload):
            if isinstance(payload, bytes):
                await websocket.send_bytes(payload)
            else:
                await websocket.send_str(payload)

        async def close(code, reason):
            await websocket.close(code=code, message=reason.encode("utf-8"))

        await serve_socket(binding, receive=receive, send=send, close=close)
        return websocket

    return handle
