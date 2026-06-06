from __future__ import annotations

import requests

from guide_excursions.kaggle_service import _is_transient_kaggle_status_error


def _http_error(status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.HTTPError(f"{status_code} error", response=response)


def test_kaggle_status_http_500_is_transient() -> None:
    assert _is_transient_kaggle_status_error(_http_error(500))
    assert _is_transient_kaggle_status_error(_http_error(503))


def test_kaggle_status_http_400_is_not_transient() -> None:
    assert not _is_transient_kaggle_status_error(_http_error(400))
