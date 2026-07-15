#!/usr/bin/env python3
"""Inspect/create Telegram forum topics and publish mobile UI review evidence."""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
from pathlib import Path
from typing import Any

from telethon import TelegramClient, functions, types, utils
from telethon.sessions import StringSession

DEFAULT_CHAT_ID = -1004337049383
DEFAULT_ENV_FILE = "/home/dev/projects/events-bot-new/.env"


def load_env(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"env file not found: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def telethon_config(env_file: Path) -> tuple[int, str, str, dict[str, str]]:
    load_env(env_file)
    api_id = int(os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "0")
    api_hash = os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or ""
    session = (os.getenv("TELEGRAM_SESSION") or "").strip()
    kwargs: dict[str, str] = {}
    bundle_b64 = (os.getenv("TELEGRAM_AUTH_BUNDLE_E2E") or "").strip()
    if bundle_b64:
        bundle = json.loads(base64.urlsafe_b64decode(bundle_b64.encode("ascii")).decode("utf-8"))
        session = (bundle.get("session") or "").strip()
        for key in ("device_model", "system_version", "app_version", "lang_code", "system_lang_code"):
            if bundle.get(key):
                kwargs[key] = str(bundle[key])
    if not api_id or not api_hash or not session:
        raise SystemExit("missing API id/hash and TELEGRAM_AUTH_BUNDLE_E2E or TELEGRAM_SESSION")
    return api_id, api_hash, session, kwargs


def dump(payload: dict[str, Any], output: str | None) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if output:
        path = Path(output).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


async def topic_messages(client: TelegramClient, entity: Any, topic_id: int, limit: int,
                         media_dir: Path | None) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    async for message in client.iter_messages(entity, limit=limit, reply_to=topic_id, reverse=True):
        media_path = None
        if media_dir and message.media:
            media_dir.mkdir(parents=True, exist_ok=True)
            media_path = await message.download_media(file=str(media_dir))
        sender = await message.get_sender()
        messages.append({
            "id": int(message.id),
            "date": message.date.isoformat() if message.date else None,
            "sender_id": int(message.sender_id) if message.sender_id else None,
            "sender_username": getattr(sender, "username", None),
            "text": message.raw_text or "",
            "has_media": bool(message.media),
            "downloaded_media": str(media_path) if media_path else None,
        })
    return messages


async def inspect_topics(client: TelegramClient, entity: Any, args: argparse.Namespace) -> dict[str, Any]:
    peer = await client.get_input_entity(entity)
    inventory: list[Any] = []
    offset_date = None
    offset_id = 0
    offset_topic = 0
    seen_ids: set[int] = set()
    while len(inventory) < args.topic_limit:
        batch_limit = min(100, args.topic_limit - len(inventory))
        result = await client(functions.messages.GetForumTopicsRequest(
            peer=peer, offset_date=offset_date, offset_id=offset_id,
            offset_topic=offset_topic, limit=batch_limit, q=None,
        ))
        batch = [topic for topic in result.topics if int(topic.id) not in seen_ids]
        if not batch:
            break
        inventory.extend(batch)
        seen_ids.update(int(topic.id) for topic in batch)
        if len(result.topics) < batch_limit:
            break
        last = result.topics[-1]
        offset_date = last.date
        offset_id = int(last.top_message)
        offset_topic = int(last.id)
    media_dir = Path(args.download_media_dir).expanduser().resolve() if args.download_media_dir else None
    topics: list[dict[str, Any]] = []
    for topic in inventory:
        if args.topic_id is not None and int(topic.id) != args.topic_id:
            continue
        record: dict[str, Any] = {
            "id": int(topic.id),
            "title": topic.title,
            "top_message": int(topic.top_message),
            "closed": bool(getattr(topic, "closed", False)),
        }
        if args.include_messages or args.topic_id is not None:
            topic_media_dir = media_dir / str(topic.id) if media_dir else None
            record["messages"] = await topic_messages(client, entity, int(topic.id), args.message_limit, topic_media_dir)
        topics.append(record)
    return {"chat_id": utils.get_peer_id(entity), "chat_title": entity.title, "forum": bool(entity.forum), "topics": topics}


def created_topic_id(result: Any) -> int | None:
    for update in getattr(result, "updates", []):
        message = getattr(update, "message", None)
        if message and isinstance(getattr(message, "action", None), types.MessageActionTopicCreate):
            return int(message.id)
    return None


async def run(args: argparse.Namespace) -> None:
    api_id, api_hash, session, kwargs = telethon_config(Path(args.env_file))
    client = TelegramClient(StringSession(session), api_id, api_hash, **kwargs)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("Telegram session is not authorized")
    me = await client.get_me()
    entity = await client.get_entity(args.chat_id)
    if not getattr(entity, "forum", False):
        raise SystemExit(f"target chat is not a forum: {args.chat_id}")

    if args.command == "inspect":
        payload = await inspect_topics(client, entity, args)
    elif args.command == "ensure-topic":
        probe = argparse.Namespace(topic_limit=500, topic_id=None, download_media_dir=None,
                                   include_messages=False, message_limit=1)
        inventory = await inspect_topics(client, entity, probe)
        exact = next((topic for topic in inventory["topics"] if topic["title"].casefold() == args.title.casefold()), None)
        created = False
        if exact is None:
            if not args.create_if_missing:
                raise SystemExit("exact topic is missing; inspect titles first, then pass --create-if-missing")
            result = await client(functions.messages.CreateForumTopicRequest(peer=entity, title=args.title))
            topic_id = created_topic_id(result)
            if topic_id is None:
                raise SystemExit("topic creation returned no MessageActionTopicCreate id")
            exact = {"id": topic_id, "title": args.title}
            created = True
        payload = {"chat_id": utils.get_peer_id(entity), "topic_id": exact["id"], "title": exact["title"], "created": created}
    else:
        sent: list[dict[str, Any]] = []
        if args.text:
            message = await client.send_message(entity, args.text, reply_to=args.topic_id, link_preview=False)
            sent.append({"id": int(message.id), "kind": "text"})
        for raw_path in args.file:
            path = Path(raw_path).expanduser().resolve()
            if not path.exists():
                raise SystemExit(f"file not found: {path}")
            message = await client.send_file(entity, str(path), caption=args.caption or "", reply_to=args.topic_id,
                                             force_document=bool(args.document))
            sent.append({"id": int(message.id), "kind": "file", "file": str(path)})
        if not sent:
            raise SystemExit("send requires --text or at least one --file")
        recent_ids = {int(message.id) async for message in client.iter_messages(entity, limit=50, reply_to=args.topic_id)}
        payload = {"chat_id": utils.get_peer_id(entity), "topic_id": args.topic_id, "sent": sent,
                   "verified": all(item["id"] in recent_ids for item in sent)}
        if not payload["verified"]:
            raise SystemExit("sent messages were not found in the topic verification window")

    payload["account_id"] = int(me.id)
    payload["account_username"] = getattr(me, "username", None)
    dump(payload, args.output)
    await client.disconnect()


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    root.add_argument("--chat-id", type=int, default=DEFAULT_CHAT_ID)
    root.add_argument("--output", help="Write the redacted JSON receipt/inspection to this artifact path")
    sub = root.add_subparsers(dest="command", required=True)

    inspect_parser = sub.add_parser("inspect", help="List topics or read a topic with comments/media")
    inspect_parser.add_argument("--topic-id", type=int)
    inspect_parser.add_argument("--topic-limit", type=int, default=500)
    inspect_parser.add_argument("--message-limit", type=int, default=200)
    inspect_parser.add_argument("--include-messages", action="store_true", help="Read messages for every listed topic")
    inspect_parser.add_argument("--download-media-dir")

    ensure = sub.add_parser("ensure-topic", help="Find an exact-title topic or explicitly create it")
    ensure.add_argument("--title", required=True)
    ensure.add_argument("--create-if-missing", action="store_true")

    send = sub.add_parser("send", help="Send review text/files into an existing topic and verify them")
    send.add_argument("--topic-id", type=int, required=True)
    send.add_argument("--text")
    send.add_argument("--file", action="append", default=[])
    send.add_argument("--caption", default="")
    send.add_argument("--document", action="store_true")
    return root


if __name__ == "__main__":
    asyncio.run(run(parser().parse_args()))
