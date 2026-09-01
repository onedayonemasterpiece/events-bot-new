import logging

import yandex_storage


class _ProviderError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("sensitive provider detail")
        self.response = {
            "ResponseMetadata": {"HTTPStatusCode": 400},
            "Error": {
                "Code": "BucketMaxSizeExceeded",
                "Message": "must not be logged",
            },
        }


def test_upload_yandex_public_bytes_logs_bounded_provider_code(caplog):
    class Client:
        def put_object(self, **_kwargs):
            raise _ProviderError()

    with caplog.at_level(logging.WARNING):
        result = yandex_storage.upload_yandex_public_bytes(
            b"poster",
            object_path="p/image/v2/example.webp",
            content_type="image/webp",
            bucket="kenigevents.ru",
            client=Client(),
        )

    assert result is None
    assert "BucketMaxSizeExceeded" in caplog.text
    assert "http_status=400" in caplog.text
    assert "bucket=kenigevents.ru" in caplog.text
    assert "path=p/image/v2/example.webp" in caplog.text
    assert "sensitive provider detail" not in caplog.text
    assert "must not be logged" not in caplog.text
