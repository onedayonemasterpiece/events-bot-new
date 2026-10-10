"""Small HTTP adapter to VibePublish's durable publication/read operations."""
from __future__ import annotations

import base64
import os
from html.parser import HTMLParser
from urllib.parse import urlsplit

import aiohttp


class SnapshotUnavailable(RuntimeError):
    pass


class _HTMLContent(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.nodes = []
        self.style = "normal"
        self.link = None

    def handle_starttag(self, tag, attrs):
        if tag in {"b", "strong"}:
            self.style = "bold"
        elif tag in {"i", "em"}:
            self.style = "italic"
        elif tag == "a":
            self.link = dict(attrs).get("href")
        elif tag == "br":
            self.handle_data("\n")

    def handle_endtag(self, tag):
        if tag == "a":
            self.link = None
        elif tag in {"b", "strong", "i", "em"}:
            self.style = "normal"

    def handle_data(self, data):
        if not data:
            return
        if self.link:
            if urlsplit(self.link).hostname in {"vk.cc", "www.vk.cc"}:
                raise SnapshotUnavailable("shortlink_not_original_destination")
            self.nodes.append({"kind": "link", "label": data, "url": self.link})
        else:
            self.nodes.append({"kind": "text", "text": data, "style": self.style})


def html_content(text):
    parser = _HTMLContent()
    parser.feed(text)
    if not parser.nodes:
        raise SnapshotUnavailable("caption_missing")
    return {"paragraphs": [parser.nodes]}


def _receipts(data):
    if not isinstance(data, dict):
        return []
    receipts = data.get("receipts")
    if isinstance(receipts, list):
        return [x for x in receipts if isinstance(x, dict)]
    return [data]


def _operation_id(data):
    for receipt in _receipts(data):
        if receipt.get("operation_id"):
            return str(receipt["operation_id"])
    return None


class VibePublishClient:
    def __init__(self, payload=None, *, base_url=None, token=None):
        self.payload = dict(payload or {})
        self.base_url = (base_url or os.getenv("GUIDE_VIBEPUBLISH_URL") or "").rstrip("/")
        self.token = token or os.getenv("GUIDE_VIBEPUBLISH_SERVICE_TOKEN") or ""
        if urlsplit(self.base_url).scheme != "https" or not self.token:
            raise SnapshotUnavailable("vibepublish_binding_missing")

    async def _request(self, method, path, *, key=None, body=None, image=None):
        headers = {"Authorization": "Bearer " + self.token}
        if key:
            headers["Idempotency-Key"] = key
        kwargs = {"json": body} if body is not None else {}
        if image is not None:
            headers["Content-Type"] = "image/jpeg"
            kwargs = {"data": image}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
            async with session.request(method, self.base_url + path, headers=headers, **kwargs) as response:
                if response.status >= 400:
                    # Never include response body, auth, provider text or signed URLs in errors.
                    raise SnapshotUnavailable("vibepublish_http_" + str(response.status))
                result = await response.json()
                if isinstance(result, dict) and isinstance(result.get("error"), dict):
                    code = str(result["error"].get("code") or "provider_error")
                    if len(code) > 80 or not code.replace("_", "").isalnum():
                        code = "provider_error"
                    raise SnapshotUnavailable(code)
                return result

    async def verify_binding(self, payload):
        bootstrap = await self._request("GET", "/v1/bootstrap")
        matches = [d for d in bootstrap.get("destinations", [])
                   if d.get("alias") == payload.get("alias") and d.get("kind") == "destination"]
        if (len(matches) != 1 or matches[0].get("provider") != "max"
                or str(matches[0].get("native_id") or "") != str(payload.get("native_id") or "")
                or int(matches[0].get("revision") or 0) != int(payload.get("binding_revision") or -1)):
            raise SnapshotUnavailable("max_binding_changed_or_unverified")

    async def _admit(self, payload, key):
        # Deployment pins this revision to the resolver-verified native identity.
        # A changed/revoked alias fails closed before uploading or admitting work.
        await self.verify_binding(payload)
        media = payload.get("media")
        if not media:
            image = base64.b64decode(payload["image_b64"], validate=True)
            asset = await self._request("POST", "/v1/assets", key=key + "-image", image=image)
            asset_id = asset.get("asset_id")
            if not asset_id:
                raise SnapshotUnavailable("asset_receipt_missing")
            media = [{"source": {"kind": "asset", "id": asset_id}, "role": "image"}]
        body = {"to": [payload["alias"]], "content": payload["content"], "media": media,
                "delivery": {"kind": "now"}, "mode": "execute", "request_key": key}
        return await self._request("POST", "/v1/publications", key=key, body=body)

    def _publication_result(self, data):
        operation_id = _operation_id(data)
        result = {"state": "accepted" if operation_id else "outcome_unknown", "operation_id": operation_id}
        for receipt in _receipts(data):
            if receipt.get("state") in {"failed", "blocked", "needs_review", "outcome_unknown"}:
                result.update(state="outcome_unknown" if receipt["state"] == "outcome_unknown" else "failed", error_code="operation_" + receipt["state"])
            for delivery in receipt.get("deliveries") or []:
                if delivery.get("destination") != self.payload.get("alias") or delivery.get("provider") != "max":
                    continue
                state, observed = delivery.get("state"), delivery.get("observed")
                if state == "verified" and observed == "published" and receipt.get("operation_complete") is True:
                    result.update(state="published", receipt={
                        "post_urls": [delivery["url"]] if delivery.get("url") else [],
                        "item_ref": delivery.get("item_ref"),
                        "transport": "vibepublish_max", "operation_id": operation_id,
                    })
                elif observed == "scheduled" and state == "verified":
                    result["state"] = "scheduled"
                elif state in {"failed", "blocked", "needs_review", "outcome_unknown"}:
                    result.update(state="outcome_unknown" if state == "outcome_unknown" else "failed",
                                  error_code="provider_" + state)
        return result

    async def submit(self, payload, key):
        self.payload = dict(payload)
        return self._publication_result(await self._admit(self.payload, key))

    async def observe(self, key, operation_id):
        if operation_id:
            data = await self._request("GET", "/v1/operations/" + operation_id)
        else:
            # The server atomically persists principal+key+intent forever. Exact
            # admission replay recovers its operation ID; it cannot dispatch twice.
            data = await self._admit(self.payload, key)
        return self._publication_result(data)
