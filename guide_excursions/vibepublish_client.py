"""Small HTTP adapter to VibePublish's durable publication/read operations."""
from __future__ import annotations

import base64
import os
import re
from datetime import datetime
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


def _remote_error_code(error, fallback):
    code = error.get('code') if isinstance(error, dict) else None
    return code if isinstance(code, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,79}', code) else fallback


class VibePublishClient:
    def __init__(self, payload=None, *, base_url=None, token=None):
        self.payload = dict(payload or {})
        self.recovery_record = {}
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

    def restore_recovery(self, record):
        self.recovery_record = dict(record) if isinstance(record, dict) else {}

    def _recovery_candidate(self, data, operation_id):
        for receipt in _receipts(data):
            if 'adjudication' in receipt:
                continue
            deliveries = receipt.get('deliveries') or []
            if (receipt.get('operation_id') != operation_id or receipt.get('action') != 'publish' or receipt.get('operation_complete') is not True
                    or receipt.get('state') not in {'blocked', 'failed'} or len(deliveries) != 1):
                continue
            delivery = deliveries[0]
            publication_id, revision = receipt.get('resource_id'), receipt.get('revision')
            if (delivery.get('destination') == self.payload.get('alias') and delivery.get('provider') == 'max'
                    and delivery.get('state') in {'blocked', 'failed'} and delivery.get('observed') == 'not_attempted'
                    and isinstance(publication_id, str) and re.fullmatch(r'pub_[a-zA-Z0-9]+', publication_id)
                    and isinstance(revision, int) and not isinstance(revision, bool) and revision > 0):
                return publication_id, revision
        return None

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


    def _owner_adjudication(self, receipt, operation_id):
        """A separate owner acknowledgment, never provider publication proof."""
        value = receipt.get("adjudication")
        required = {"adjudication_id", "disposition", "original_operation_id", "attempt_id",
                    "destination", "native_target", "native_id", "item_ref", "url",
                    "media_sha256", "recorded_at", "quarantine_release", "original_outcome",
                    "replay_allowed"}
        if not isinstance(value, dict) or set(value) != required:
            return None
        deliveries = receipt.get("deliveries")
        if (receipt.get("state") != "outcome_unknown" or receipt.get("operation_complete") is not True
                or receipt.get("action") != "publish" or receipt.get("operation_id") != operation_id
                or not isinstance(deliveries, list) or len(deliveries) != 1):
            return None
        delivery = deliveries[0]
        if (not isinstance(delivery, dict) or delivery.get("destination") != self.payload.get("alias")
                or delivery.get("provider") != "max" or delivery.get("state") != "outcome_unknown"
                or value.get("disposition") != "owner_confirmed_present"
                or value.get("original_operation_id") != operation_id
                or value.get("destination") != self.payload.get("alias")
                or str(value.get("native_target")) != str(self.payload.get("native_id"))
                or value.get("original_outcome") != "outcome_unknown"
                or value.get("replay_allowed") is not False
                or value.get("quarantine_release") not in {"pending", "done"}):
            return None
        for field in ("adjudication_id", "attempt_id", "native_id", "item_ref"):
            if not isinstance(value.get(field), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", value[field]):
                return None
        if delivery.get("attempt_id") != value["attempt_id"]:
            return None
        hashes = value.get("media_sha256")
        if (not isinstance(hashes, list) or len(hashes) != 1
                or not all(isinstance(h, str) and re.fullmatch(r"[0-9a-f]{64}", h) for h in hashes)):
            return None
        try:
            url = urlsplit(value["url"])
            port = url.port
            instant = datetime.fromisoformat(value["recorded_at"].replace("Z", "+00:00"))
        except (TypeError, ValueError, AttributeError):
            return None
        if (url.scheme != "https" or url.hostname != "max.ru" or url.username or url.password
                or port or url.query or url.fragment or instant.tzinfo is None
                or not (url.path == f"/c/{self.payload.get('native_id')}/{value['native_id']}"
                        or re.fullmatch(r"/[A-Za-z0-9_]{1,64}/" + re.escape(value["native_id"]), url.path))):
            return None
        return dict(value)

    def _publication_result(self, data):
        operation_id = _operation_id(data)
        result = {"state": "accepted" if operation_id else "outcome_unknown", "operation_id": operation_id}
        for receipt in _receipts(data):
            if receipt.get("state") in {"failed", "blocked", "needs_review", "outcome_unknown"}:
                result.update(state="outcome_unknown" if receipt["state"] == "outcome_unknown" else "failed", error_code=_remote_error_code(receipt.get("error"), "operation_" + receipt["state"]))
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
                                  error_code=_remote_error_code(delivery.get("error"), result.get("error_code") or "provider_" + state))
        for receipt in _receipts(data):
            adjudication = self._owner_adjudication(receipt, operation_id)
            if adjudication is not None:
                result.update(state="owner_confirmed_present" if adjudication["quarantine_release"] == "done" else "outcome_unknown",
                              error_code=_remote_error_code(receipt.get("error"), "max_outcome_unknown"),
                              receipt={"transport": "vibepublish_max", "operation_id": operation_id,
                                       "verification": "owner_confirmed_present",
                                       "post_urls": [adjudication["url"]], "item_ref": adjudication["item_ref"],
                                       "adjudication": adjudication})
                break
        if self.recovery_record:
            result.setdefault('receipt', {})['recovery'] = dict(self.recovery_record)
            if result.get('state') == 'failed' and self.recovery_record.get('state') == 'rejected':
                result['error_code'] = self.recovery_record.get('error_code') or result.get('error_code')
        return result

    async def submit(self, payload, key):
        self.payload = dict(payload)
        return self._publication_result(await self._admit(self.payload, key))

    async def observe(self, key, operation_id):
        if operation_id:
            data = await self._request("GET", "/v1/operations/" + operation_id)
            if _operation_id(data) != operation_id:
                raise SnapshotUnavailable("operation_identity_mismatch")
            candidate = self._recovery_candidate(data, operation_id)
            if candidate and self.recovery_record.get('state') in {None, 'requested'}:
                # Explicit recovery of the SAME frozen operation, never a new publish.
                # Server atomically requires dispatched=0 and makes this key a
                # single admission forever. Lost replies replay that same key.
                publication_id, revision = candidate
                if self.recovery_record and (self.recovery_record.get('publication_id'), self.recovery_record.get('revision')) != candidate:
                    raise SnapshotUnavailable('recovery_identity_changed')
                await self.verify_binding(self.payload)
                recovery_key = key + '-recover-v1'
                self.recovery_record = {'request_key': recovery_key, 'publication_id': publication_id,
                                        'revision': revision, 'state': 'requested'}
                try:
                    recovered = await self._request('POST', '/v1/publications/' + publication_id + '/commands',
                        key=recovery_key, body={'expected_revision': revision,
                                               'change': {'kind': 'retry_failed', 'destinations': [self.payload['alias']]}})
                except SnapshotUnavailable as exc:
                    code = str(exc)
                    if code in {'vibepublish_http_408', 'vibepublish_http_429'} or code.startswith('vibepublish_http_5'):
                        # Ambiguous/transient response: preserve the recovery key;
                        # next observation may only reconcile that same admission.
                        raise
                    # A definite refusal is recorded once; do not loop on denial.
                    self.recovery_record.update(state='rejected', error_code=code)
                    return self._publication_result(data)
                self.recovery_record['state'] = 'acknowledged'
                if _operation_id(recovered) != operation_id:
                    result = self._publication_result(data)
                    result['error_code'] = 'recovery_operation_identity_changed'
                    return result
                data = recovered
        else:
            # The server atomically persists principal+key+intent forever. Exact
            # admission replay recovers its operation ID; it cannot dispatch twice.
            data = await self._admit(self.payload, key)
        return self._publication_result(data)
