from __future__ import annotations

import argparse

import pytest

from scripts.run_cherryflash_live import _validate_args


def test_validate_args_rejects_no_wait() -> None:
    args = argparse.Namespace(wait=False)

    with pytest.raises(RuntimeError, match="does not support --no-wait"):
        _validate_args(args)


def test_validate_args_allows_wait_mode() -> None:
    args = argparse.Namespace(wait=True)

    _validate_args(args)
