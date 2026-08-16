import asyncio
import logging
import os
import sys
from types import SimpleNamespace

import pytest
from aiogram import types

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main
import vision_test
import vision_test.ocr as vision_ocr
from vision_test import OcrResult
from vision_test import session as vision_session
from main import Database, User


MINIMAL_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x03\x01\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
)


@pytest.fixture(autouse=True)
def reset_vision_sessions():
    vision_session.reset_sessions()
    yield
    vision_session.reset_sessions()


class DummyBot:
    def __init__(self):
        self.messages: list[SimpleNamespace] = []

    async def send_message(self, chat_id, text, **kwargs):
        msg = SimpleNamespace(
            message_id=len(self.messages) + 1,
            date=0,
            chat=SimpleNamespace(id=chat_id, type="private"),
            from_user=SimpleNamespace(id=0, is_bot=True, first_name="B"),
            text=text,
            reply_markup=kwargs.get("reply_markup"),
            parse_mode=kwargs.get("parse_mode"),
        )
        self.messages.append(msg)
        return msg


class DummySession:
    async def post(self, *args, **kwargs):  # pragma: no cover - start only stores reference
        raise AssertionError("run_ocr should be stubbed in tests")


class DummyMessage:
    def __init__(self):
        self.calls: list = []

    async def edit_reply_markup(self, reply_markup=None):
        self.calls.append(reply_markup)


class DummyCallback:
    def __init__(self, user_id: int, data: str, message):
        self.from_user = SimpleNamespace(id=user_id)
        self.data = data
        self.message = message
        self.answers: list[tuple[str | None, bool]] = []

    async def answer(self, text: str | None = None, show_alert: bool = False):
        self.answers.append((text, show_alert))


@pytest.mark.asyncio
async def test_ocrtest_start_superadmin(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=1, is_superadmin=True))
        await session.commit()

    msg = types.Message.model_validate(
        {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 1, "type": "private"},
            "from": {"id": 1, "is_bot": False, "first_name": "U"},
            "text": "/ocrtest",
        }
    )

    bot = DummyBot()
    monkeypatch.setattr(main, "get_http_session", lambda: DummySession())

    await main.handle_ocrtest(msg, db, bot)
    assert bot.messages, "no message sent"
    markup = bot.messages[0].reply_markup
    assert markup is not None
    buttons = [btn.text for row in markup.inline_keyboard for btn in row]
    assert any(text.startswith("Сменить детализацию") for text in buttons)
    assert "Завершить" in buttons


@pytest.mark.asyncio
async def test_ocrtest_denies_non_admin(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=2, is_superadmin=False))
        await session.commit()

    msg = types.Message.model_validate(
        {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 2, "type": "private"},
            "from": {"id": 2, "is_bot": False, "first_name": "U"},
            "text": "/ocrtest",
        }
    )

    bot = DummyBot()
    monkeypatch.setattr(main, "get_http_session", lambda: DummySession())

    await main.handle_ocrtest(msg, db, bot)
    assert bot.messages
    assert bot.messages[0].text == "Not authorized"


@pytest.mark.asyncio
async def test_select_detail_updates_state(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=3, is_superadmin=True))
        await session.commit()

    msg = types.Message.model_validate(
        {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 3, "type": "private"},
            "from": {"id": 3, "is_bot": False, "first_name": "U"},
            "text": "/ocrtest",
        }
    )

    bot = DummyBot()
    monkeypatch.setattr(main, "get_http_session", lambda: DummySession())
    await main.handle_ocrtest(msg, db, bot)

    dummy_message = DummyMessage()
    callback = DummyCallback(3, "ocr:detail:low", dummy_message)
    await vision_test.select_detail(callback, bot)

    session = vision_session.get_session(3)
    assert session is not None
    assert session.detail == "low"
    assert callback.answers
    assert session.waiting_for_photo


@pytest.mark.asyncio
async def test_handle_photo_uses_both_models(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(User(user_id=4, is_superadmin=True))
        await session.commit()

    msg = types.Message.model_validate(
        {
            "message_id": 1,
            "date": 0,
            "chat": {"id": 4, "type": "private"},
            "from": {"id": 4, "is_bot": False, "first_name": "U"},
            "text": "/ocrtest",
        }
    )

    bot = DummyBot()
    monkeypatch.setattr(main, "get_http_session", lambda: DummySession())
    await main.handle_ocrtest(msg, db, bot)

    calls: list[tuple[str, str]] = []
    usage_logs: list[tuple[str, str, dict, str, str | None, dict]] = []

    async def fake_run_ocr(image: bytes, *, model: str, detail: str):
        calls.append((model, detail))
        if model == "gpt-4o-mini":
            return OcrResult(
                text="строка одна\nобщая",
                usage=vision_ocr.OcrUsage(
                    prompt_tokens=10,
                    completion_tokens=5,
                    total_tokens=15,
                ),
            )
        return OcrResult(
            text="строка одна\nдругая",
            usage=vision_ocr.OcrUsage(
                prompt_tokens=20,
                completion_tokens=7,
                total_tokens=27,
            ),
            )

    monkeypatch.setattr(vision_test, "run_ocr", fake_run_ocr)
    monkeypatch.setattr(vision_test, "_USAGE_LOGGER_CONFIGURED", False)
    monkeypatch.setattr(vision_test, "_LOG_TOKEN_USAGE", None)
    monkeypatch.setattr(vision_test, "_BOT_CODE", None)

    async def fake_log(bot_code, model, usage, *, endpoint, request_id, meta):
        usage_logs.append((bot_code, model, usage, endpoint, request_id, dict(meta)))

    monkeypatch.setattr(main, "log_token_usage", fake_log)

    photo_msg = types.Message.model_validate(
        {
            "message_id": 2,
            "date": 0,
            "chat": {"id": 4, "type": "private"},
            "from": {"id": 4, "is_bot": False, "first_name": "U"},
        }
    )

    await vision_test.handle_photo(photo_msg, bot, [(b"jpegdata", "poster.jpg")])

    assert calls == [
        ("gpt-4o-mini", "auto"),
        ("gpt-4o", "auto"),
    ]
    assert bot.messages
    text = bot.messages[-1].text
    assert "gpt-4o-mini" in text
    assert "prompt" in text
    assert "15" in text
    assert "27" in text
    assert "Схожесть:" in text
    assert "Различия" in text
    session = vision_session.get_session(4)
    assert session is not None
    assert session.last_texts.get("gpt-4o")
    assert session.waiting_for_photo
    assert len(usage_logs) == 2
    assert [entry[0] for entry in usage_logs] == [main.BOT_CODE, main.BOT_CODE]
    assert [entry[1] for entry in usage_logs] == ["gpt-4o-mini", "gpt-4o"]
    assert all(entry[3] == "chat.completions" for entry in usage_logs)
    assert all(entry[5]["detail"] == "auto" for entry in usage_logs)
    assert all(entry[5]["file"] == "poster.jpg" for entry in usage_logs)
    assert all(entry[5]["source"] == "vision_test.handle_photo" for entry in usage_logs)
    assert all(entry[2]["total_tokens"] in {15, 27} for entry in usage_logs)


@pytest.mark.asyncio
async def test_run_ocr_logs_body_snippet(monkeypatch, caplog):
    class FakeResponse:
        def __init__(self, *, status: int, body: str, headers: dict[str, str] | None = None):
            self.status = status
            self._body = body
            self.headers = headers or {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def text(self):
            return self._body

        async def json(self):  # pragma: no cover - not used in error path
            return {}

    class FakeSession:
        def __init__(self, response: FakeResponse):
            self._response = response

        def post(self, *args, **kwargs):
            return self._response

    monkeypatch.setenv("FOUR_O_TOKEN", "token")
    monkeypatch.setenv("FOUR_O_URL", "https://example.test")

    response = FakeResponse(
        status=400,
        body="{\"error\":\"synthetic failure\"}",
        headers={"x-request-id": "req-123"},
    )
    session = FakeSession(response)
    semaphore = asyncio.Semaphore(1)

    vision_ocr.configure_http(session=session, semaphore=semaphore)

    try:
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError) as excinfo:
                await vision_ocr.run_ocr(MINIMAL_PNG, model="test-model", detail="high")

        assert "synthetic failure" in str(excinfo.value)
        assert any("synthetic failure" in record.getMessage() for record in caplog.records)
    finally:
        vision_ocr.clear_http()


@pytest.mark.asyncio
async def test_run_ocr_payload_structure(monkeypatch):
    class CapturingResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        @property
        def headers(self):
            return {}

        async def json(self):
            return {
                "choices": [{"message": {"content": "распознанный текст"}}],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 2,
                    "total_tokens": 3,
                },
            }

    class CapturingSession:
        def __init__(self):
            self.calls = []

        def post(self, url, *, json, headers):
            self.calls.append((url, json, headers))
            return CapturingResponse()

    session = CapturingSession()
    semaphore = asyncio.Semaphore(1)
    monkeypatch.setenv("FOUR_O_TOKEN", "token")
    monkeypatch.setenv("FOUR_O_URL", "https://example.test/v1/chat/completions")
    vision_ocr.configure_http(session=session, semaphore=semaphore)

    try:
        result = await vision_test.run_ocr(MINIMAL_PNG, model="gpt-4o", detail="auto")
    finally:
        vision_ocr.clear_http()

    assert result.text == "распознанный текст"
    assert result.usage.prompt_tokens == 1
    assert result.usage.completion_tokens == 2
    assert result.usage.total_tokens == 3
    assert len(session.calls) == 1
    url, payload, headers = session.calls[0]
    assert url == "https://example.test/v1/chat/completions"
    assert headers["Authorization"] == "Bearer token"
    content = payload["messages"][1]["content"]
    assert isinstance(content, list)
    assert content[0]["type"] == "text"
    assert content[0]["text"].startswith("Распознай текст на изображении.")
    assert all(item.get("type") != "input_text" for item in content)


@pytest.mark.asyncio
async def test_run_ocr_retries_timeout_then_succeeds_with_distinct_request_ids(monkeypatch):
    class SuccessResponse:
        status = 200
        headers = {"x-request-id": "server-request"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {
                "choices": [{"message": {"content": '{"poster_ocr_text":"афиша","ocr_title":""}'}}],
                "usage": {},
            }

    class TimeoutThenSuccessSession:
        def __init__(self):
            self.calls = []

        def post(self, url, *, json, headers):
            self.calls.append(dict(headers))
            if len(self.calls) == 1:
                raise asyncio.TimeoutError("synthetic timeout")
            return SuccessResponse()

    async def no_sleep(_delay):
        return None

    monkeypatch.setenv("FOUR_O_TOKEN", "token")
    monkeypatch.setenv("FOUR_O_OCR_MAX_ATTEMPTS", "3")
    monkeypatch.setattr(vision_ocr.asyncio, "sleep", no_sleep)
    session = TimeoutThenSuccessSession()
    vision_ocr.configure_http(session=session, semaphore=asyncio.Semaphore(1))
    try:
        result = await vision_ocr.run_ocr(MINIMAL_PNG, model="test-model", detail="high")
    finally:
        vision_ocr.clear_http()

    assert result.text == "афиша"
    assert len(session.calls) == 2
    request_ids = [call["X-Client-Request-Id"] for call in session.calls]
    assert all(value.startswith("ocr-") for value in request_ids)
    assert len(set(request_ids)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [408, 409, 429, 500, 503])
async def test_run_ocr_retries_only_retryable_http_status(monkeypatch, status):
    class Response:
        def __init__(self, response_status):
            self.status = response_status
            self.headers = {"Retry-After": "0"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def text(self):
            return "temporary"

        async def json(self):
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

    class Session:
        def __init__(self):
            self.calls = 0

        def post(self, *_args, **_kwargs):
            self.calls += 1
            return Response(status if self.calls == 1 else 200)

    monkeypatch.setenv("FOUR_O_TOKEN", "token")
    monkeypatch.setenv("FOUR_O_OCR_MAX_ATTEMPTS", "3")
    session = Session()
    vision_ocr.configure_http(session=session, semaphore=asyncio.Semaphore(1))
    try:
        result = await vision_ocr.run_ocr(MINIMAL_PNG, model="test-model", detail="high")
    finally:
        vision_ocr.clear_http()
    assert result.text == "ok"
    assert session.calls == 2


@pytest.mark.asyncio
async def test_run_ocr_does_not_retry_nonretryable_http_status(monkeypatch):
    class Response:
        status = 400
        headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def text(self):
            return "invalid image"

    class Session:
        def __init__(self):
            self.calls = 0

        def post(self, *_args, **_kwargs):
            self.calls += 1
            return Response()

    monkeypatch.setenv("FOUR_O_TOKEN", "token")
    session = Session()
    vision_ocr.configure_http(session=session, semaphore=asyncio.Semaphore(1))
    try:
        with pytest.raises(RuntimeError, match="status 400"):
            await vision_ocr.run_ocr(MINIMAL_PNG, model="test-model", detail="high")
    finally:
        vision_ocr.clear_http()
    assert session.calls == 1


@pytest.mark.asyncio
async def test_run_ocr_exhausts_bounded_attempts(monkeypatch):
    class Session:
        def __init__(self):
            self.calls = 0

        def post(self, *_args, **_kwargs):
            self.calls += 1
            raise asyncio.TimeoutError("still unavailable")

    async def no_sleep(_delay):
        return None

    monkeypatch.setenv("FOUR_O_TOKEN", "token")
    monkeypatch.setenv("FOUR_O_OCR_MAX_ATTEMPTS", "3")
    monkeypatch.setattr(vision_ocr.asyncio, "sleep", no_sleep)
    session = Session()
    vision_ocr.configure_http(session=session, semaphore=asyncio.Semaphore(1))
    try:
        with pytest.raises(RuntimeError, match="after 3 attempts"):
            await vision_ocr.run_ocr(MINIMAL_PNG, model="test-model", detail="high")
    finally:
        vision_ocr.clear_http()
    assert session.calls == 3
