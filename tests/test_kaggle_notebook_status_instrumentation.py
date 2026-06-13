import json

from video_announce.kaggle_client import (
    _instrument_notebook_kernel,
    _instrument_script_kernel,
)


def _write_minimal_notebook(path):
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": ["print('hello')\n"],
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )


def test_notebook_status_instrumentation_is_idempotent(tmp_path):
    nb_path = tmp_path / "kernel.ipynb"
    _write_minimal_notebook(nb_path)
    meta = {"code_file": "kernel.ipynb", "title": "Test Kernel"}

    _instrument_notebook_kernel(tmp_path, meta)
    _instrument_notebook_kernel(tmp_path, meta)

    notebook = json.loads(nb_path.read_text(encoding="utf-8"))
    tagged = [
        cell
        for cell in notebook["cells"]
        if cell.get("metadata", {}).get("events_bot_kaggle_status")
    ]
    assert len(tagged) == 3

    bootstrap = "".join(tagged[0]["source"])
    terminal = "".join(tagged[-1]["source"])
    assert "kernel_started" in bootstrap
    assert "acquire_resource" in bootstrap
    assert "release_resource" in bootstrap
    assert "report_written" in terminal


def test_script_status_instrumentation_wraps_plain_script(tmp_path):
    script = tmp_path / "kernel.py"
    script.write_text("print('plain script')\n", encoding="utf-8")
    meta = {"code_file": "kernel.py", "title": "Script Kernel"}

    _instrument_script_kernel(tmp_path, meta)

    wrapper = script.read_text(encoding="utf-8")
    original = tmp_path / "_events_bot_original_kernel.py"
    assert original.exists()
    assert "kernel_started" in wrapper
    assert "report_written" in wrapper
    assert "runpy.run_path" in wrapper


def test_script_status_instrumentation_skips_status_aware_script(tmp_path):
    script = tmp_path / "kernel.py"
    script.write_text("from kaggle_status_client import load_status_client\n", encoding="utf-8")
    meta = {"code_file": "kernel.py", "title": "Script Kernel"}

    _instrument_script_kernel(tmp_path, meta)

    assert not (tmp_path / "_events_bot_original_kernel.py").exists()
